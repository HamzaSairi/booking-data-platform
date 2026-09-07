"""Extraction incrémentale Postgres -> Parquet, pilotée par watermark.

Garantie : at-least-once. Le Parquet est écrit AVANT que le watermark
n'avance. Un plantage entre les deux fait relire des lignes au prochain
tour (doublon, absorbé par la déduplication du jour 17) ; l'ordre inverse
les perdrait définitivement.

Bibliothèque autonome : aucune dépendance à Airflow. La configuration
vient de l'environnement, que ce soit `.env` en local ou les variables
injectées par Compose côté orchestrateur.
"""

import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from psycopg import conninfo

load_dotenv()

TABLES = ["hotels", "customers", "bookings", "payments"]

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Postgres date une ligne au DÉBUT de sa transaction, pas au commit. Une
# transaction longue peut donc écrire une ligne datée d'avant le passage
# du pipeline, et devenir invisible à jamais. On recule la borne pour la
# rattraper : on relit un peu, on ne perd rien.
SAFETY_MARGIN = timedelta(seconds=5)

# Ancrées sur la racine du dépôt, jamais sur le répertoire courant :
# Airflow exécute ses tâches depuis /opt/airflow.
RACINE = Path(__file__).resolve().parents[1]
STATE_DIR = RACINE / "state" / "watermarks"
DATA_DIR = RACINE / "data" / "raw"


def rel(chemin: Path) -> str:
    """Chemin relatif à la racine : le même sur l'hôte et dans le conteneur."""
    return chemin.relative_to(RACINE).as_posix()


# ─── Connexion ───────────────────────────────────────────────────────
def connect() -> psycopg.Connection:
    try:
        info = conninfo.make_conninfo(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ["POSTGRES_DB"],
        )
    except KeyError as exc:
        # RuntimeError et non SystemExit : une bibliothèque lève des
        # exceptions, seuls les points d'entrée décident de sortir.
        raise RuntimeError(f"Variable d'environnement manquante : {exc.args[0]}") from exc
    return psycopg.connect(info)


def db_identity(conn) -> str:
    """Identifiant unique du cluster Postgres, régénéré par initdb.

    Permet de détecter qu'un `docker compose down -v` a recréé la base
    alors que le fichier d'état, lui, vit sur le disque hôte et survit.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT system_identifier::text FROM pg_control_system()")
        return cur.fetchone()[0]


# ─── État, un fichier par table ──────────────────────────────────────
# Un fichier unique serait écrasé par le dernier des quatre extracts
# lancés en parallèle par le DAG. On partitionne au lieu de verrouiller.
def fichier_etat(table: str) -> Path:
    return STATE_DIR / f"{table}.json"


def charger_etat(table: str) -> dict:
    f = fichier_etat(table)
    if not f.exists():
        return {"db_id": None, "watermark": None}
    return json.loads(f.read_text(encoding="utf-8"))


def sauver_etat(table: str, etat: dict) -> None:
    """Écriture atomique : un plantage en cours d'écriture ne doit pas
    laisser un JSON tronqué, qui rendrait l'état illisible au prochain tour.
    """
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    cible = fichier_etat(table)
    tmp = cible.with_suffix(".tmp")
    tmp.write_text(json.dumps(etat, indent=2, default=str), encoding="utf-8")
    tmp.replace(cible)


# ─── Extraction ──────────────────────────────────────────────────────
def extract_table(conn, table: str, watermark: datetime):
    """Retourne (lignes, colonnes, nouveau_watermark) ou None si rien."""
    borne = watermark - SAFETY_MARGIN

    with conn.cursor() as cur:
        # f-string acceptable : `table` vient d'une liste codée en dur,
        # jamais d'une entrée utilisateur.
        cur.execute(
            f"SELECT * FROM {table} WHERE updated_at > %s ORDER BY updated_at",
            (borne,),
        )
        colonnes = [d.name for d in cur.description]
        lignes = cur.fetchall()

    if not lignes:
        return None

    # Le nouveau watermark est le max RÉELLEMENT extrait, jamais now() :
    # avec now(), tout ce qui est commité pendant l'exécution est perdu.
    idx = colonnes.index("updated_at")
    nouveau = max(ligne[idx] for ligne in lignes)

    # Garde-fou : une source peut écrire un updated_at dans le futur (bug
    # applicatif, horloge décalée, colonne technique dérivée d'une date
    # métier). Le watermark le mémoriserait et ignorerait ensuite toutes les
    # lignes réelles jusqu'à ce que l'horloge le rattrape — sans erreur.
    maintenant = datetime.now(UTC)
    if nouveau > maintenant:
        print(f"  ! {table} : updated_at futur ({nouveau}), watermark plafonné")
        nouveau = maintenant

    return lignes, colonnes, nouveau


def ecrire_parquet(table: str, lignes: list, colonnes: list[str]) -> Path:
    donnees = {c: [ligne[i] for ligne in lignes] for i, c in enumerate(colonnes)}
    arrow = pa.table(donnees)

    dt = datetime.now(UTC)
    # Convention Hive dt=YYYY-MM-DD, comprise par BigQuery et dbt.
    dossier = DATA_DIR / table / f"dt={dt:%Y-%m-%d}"
    dossier.mkdir(parents=True, exist_ok=True)
    # Horodatage dans le nom : deux exécutions le même jour ne s'écrasent pas.
    chemin = dossier / f"part-{dt:%Y%m%dT%H%M%S%f}.parquet"

    pq.write_table(arrow, chemin, compression="snappy")
    return chemin


def extract_one(table: str) -> list[str]:
    """Extrait une table. Retourne les chemins relatifs écrits (0 ou 1)."""
    etat = charger_etat(table)

    with connect() as conn:
        identite = db_identity(conn)
        if etat["db_id"] is None:
            etat["db_id"] = identite
        elif etat["db_id"] != identite:
            raise RuntimeError(
                f"L'état de {table} appartient à une autre base ({etat['db_id']}) "
                f"que celle connectée ({identite}). La base a probablement été "
                f"recréée par `down -v`. Supprime {fichier_etat(table)}."
            )

        wm = datetime.fromisoformat(etat["watermark"]) if etat["watermark"] else EPOCH
        resultat = extract_table(conn, table, wm)

    if resultat is None:
        print(f"{table:>10} : 0 ligne")
        return []

    lignes, colonnes, nouveau = resultat

    chemin = ecrire_parquet(table, lignes, colonnes)   # 1. écrire
    etat["watermark"] = nouveau.isoformat()            # 2. avancer
    sauver_etat(table, etat)                           # 3. persister

    print(f"{table:>10} : {len(lignes):>5} lignes -> {rel(chemin)}")
    return [rel(chemin)]


# ─── Point d'entrée CLI ──────────────────────────────────────────────
def run() -> None:
    fichiers = [f for table in TABLES for f in extract_one(table)]
    print(f"\n{len(fichiers)} fichier(s) écrit(s)")


if __name__ == "__main__":
    sys.exit(run())