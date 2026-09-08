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
    
# ─── Extraction ──────────────────────────────────────────────────────
def chemin_partition(table: str, debut: datetime) -> Path:
    """Chemin déterministe : la même fenêtre écrit toujours le même fichier.

    L'ancien nom horodaté sur l'heure d'exécution garantissait que « deux
    exécutions le même jour ne s'écrasent pas » — c'était précisément le
    défaut. Rejouer une fenêtre doit remplacer son fichier, pas en ajouter
    un second à charger deux fois.
    """
    return DATA_DIR / table / f"dt={debut:%Y-%m-%d}" / f"part-{debut:%Y%m%dT%H%M%SZ}.parquet"


def extract_table(conn, table: str, debut: datetime, fin: datetime):
    """Retourne (lignes, colonnes) pour la fenêtre [debut, fin), ou None."""
    with conn.cursor() as cur:
        # f-string acceptable : `table` vient d'une liste codée en dur,
        # jamais d'une entrée utilisateur.
        cur.execute(
            f"SELECT * FROM {table} "
            "WHERE updated_at >= %(debut)s AND updated_at < %(fin)s "
            "ORDER BY updated_at",
            {"debut": debut, "fin": fin},
        )
        colonnes = [d.name for d in cur.description]
        lignes = cur.fetchall()
    return (lignes, colonnes) if lignes else None


def ecrire_parquet(table: str, lignes: list, colonnes: list[str], debut: datetime) -> Path:
    donnees = {c: [ligne[i] for ligne in lignes] for i, c in enumerate(colonnes)}
    arrow = pa.table(donnees)
    chemin = chemin_partition(table, debut)
    chemin.parent.mkdir(parents=True, exist_ok=True)
    # Écriture puis renommage : un plantage en cours d'écriture ne laisse
    # jamais un Parquet tronqué à l'emplacement final.
    tmp = chemin.with_suffix(".parquet.tmp")
    pq.write_table(arrow, tmp, compression="snappy")
    tmp.replace(chemin)
    return chemin


def extract_one(table: str, debut: datetime, fin: datetime) -> list[str]:
    """Extrait la fenêtre [debut, fin). Retourne les chemins écrits (0 ou 1).

    Fonction pure du couple (source, fenêtre) : aucun état lu, aucun état
    écrit. Rejouer la même fenêtre réécrit le même fichier, reflétant
    l'état ACTUEL de la source — y compris les lignes arrivées en retard.
    """
    with connect() as conn:
        resultat = extract_table(conn, table, debut, fin)

    if resultat is None:
        # Une fenêtre vidée doit le rester : sinon un ancien fichier
        # survivrait à la disparition de ses lignes en source.
        chemin_partition(table, debut).unlink(missing_ok=True)
        print(f"{table:>10} [{debut:%F}] : 0 ligne")
        return []

    lignes, colonnes = resultat
    chemin = ecrire_parquet(table, lignes, colonnes, debut)
    print(f"{table:>10} [{debut:%F}] : {len(lignes)} lignes -> {rel(chemin)}")
    return [rel(chemin)]


# ─── Point d'entrée CLI ──────────────────────────────────────────────
def run(debut: datetime, fin: datetime) -> None:
    for table in TABLES:
        extract_one(table, debut, fin)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Extraction d'une fenêtre temporelle.")
    p.add_argument("--debut", required=True, help="ISO 8601, ex. 2026-09-03T00:00:00+00:00")
    p.add_argument("--fin", required=True)
    a = p.parse_args()
    run(datetime.fromisoformat(a.debut), datetime.fromisoformat(a.fin))