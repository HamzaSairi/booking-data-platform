"""Extraction incrémentale Postgres -> Parquet, pilotée par watermark.

Garantie : at-least-once. Le Parquet est écrit AVANT que le watermark
n'avance. Un plantage entre les deux fait relire des lignes au prochain
tour (doublon, absorbé par la déduplication du Jour 17) ; l'ordre inverse
les perdrait définitivement.
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

# Postgres date une ligne au DEBUT de sa transaction, pas au commit. Une
# transaction longue peut donc écrire une ligne datée d'avant le passage
# du pipeline, et devenir invisible à jamais. On recule la borne pour la
# rattraper : on relit un peu, on ne perd rien.
SAFETY_MARGIN = timedelta(seconds=5)

STATE_FILE = Path("state/watermarks.json")
DATA_DIR = Path("data/raw")


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
        raise SystemExit(f"Variable manquante dans .env : {exc.args[0]}") from exc
    return psycopg.connect(info)


def db_identity(conn) -> str:
    """Identifiant unique du cluster Postgres, régénéré par initdb.

    Permet de détecter qu'un `docker compose down -v` a recréé la base
    alors que le fichier d'état, lui, vit sur le disque hôte et survit.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT system_identifier::text FROM pg_control_system()")
        return cur.fetchone()[0]


# ─── État ────────────────────────────────────────────────────────────
def charger_etat() -> dict:
    if not STATE_FILE.exists():
        return {"db_id": None, "watermarks": {}}
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def sauver_etat(etat: dict) -> None:
    """Écriture atomique : un plantage en cours d'écriture ne doit pas
    laisser un JSON tronqué, qui rendrait l'état illisible au prochain tour.
    """
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(etat, indent=2, default=str), encoding="utf-8")
    tmp.replace(STATE_FILE)


def lire_watermark(etat: dict, table: str) -> datetime:
    valeur = etat["watermarks"].get(table)
    return datetime.fromisoformat(valeur) if valeur else EPOCH


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


# ─── Orchestration ───────────────────────────────────────────────────
def run() -> None:
    etat = charger_etat()

    with connect() as conn:
        identite = db_identity(conn)

        if etat["db_id"] is None:
            etat["db_id"] = identite
        elif etat["db_id"] != identite:
            raise SystemExit(
                f"Le fichier d'état appartient à une autre base "
                f"({etat['db_id']}) que celle connectée ({identite}).\n"
                f"La base a probablement été recréée par `down -v`.\n"
                f"Supprime {STATE_FILE} pour repartir de zéro."
            )

        total = 0
        for table in TABLES:
            resultat = extract_table(conn, table, lire_watermark(etat, table))

            if resultat is None:
                print(f"{table:>10} : 0 ligne")
                continue

            lignes, colonnes, nouveau = resultat

            chemin = ecrire_parquet(table, lignes, colonnes)   # 1. écrire
            etat["watermarks"][table] = nouveau.isoformat()    # 2. avancer
            sauver_etat(etat)                                  # 3. persister

            total += len(lignes)
            print(f"{table:>10} : {len(lignes):>5} lignes -> {chemin}")

    print(f"\nTotal : {total} ligne(s) extraite(s)")


if __name__ == "__main__":
    sys.exit(run())