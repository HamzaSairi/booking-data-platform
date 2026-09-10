"""Extraction Postgres -> Parquet, bornée par une fenêtre temporelle.

Fonction pure du couple (source, fenêtre) : aucun état lu ni écrit. Le
registre des intervalles traités appartient à l'ordonnanceur (ADR-012).
Rejouer une fenêtre réécrit le même fichier au même chemin, reflétant
l'état actuel de la source — y compris les lignes arrivées en retard.

Bibliothèque autonome : aucune dépendance à Airflow. La configuration
vient de l'environnement, `.env` en local ou variables injectées par
Compose côté orchestrateur.
"""

import os
from datetime import datetime
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from psycopg import conninfo

load_dotenv()

TABLES = ["hotels", "customers", "bookings", "payments"]

# Ancrées sur la racine du dépôt, jamais sur le répertoire courant :
# Airflow exécute ses tâches depuis /opt/airflow.
RACINE = Path(__file__).resolve().parents[1]
DATA_DIR = RACINE / "data" / "raw"


def rel(chemin: Path) -> str:
    """Chemin relatif à la racine : le même sur l'hôte et dans le conteneur."""
    return chemin.relative_to(RACINE).as_posix()


# ─── Connexion ───────────────────────────────────────────────────────
# Timeout de connexion explicite (ADR-020). Sans lui, psycopg 3.3.4 attend
# 130 s (constante privée _DEFAULT_CONNECT_TIMEOUT), mesuré au jour 15
# sur un Postgres gelé. Une extraction complète dure moins d'1 s.
CONNECT_TIMEOUT_S = 10


def connect() -> psycopg.Connection:
    try:
        info = conninfo.make_conninfo(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ["POSTGRES_DB"],
            connect_timeout=CONNECT_TIMEOUT_S,
        )
    except KeyError as exc:
        # RuntimeError et non SystemExit : une bibliothèque lève des
        # exceptions, seuls les points d'entrée décident de sortir.
        raise RuntimeError(f"Variable d'environnement manquante : {exc.args[0]}") from exc
    return psycopg.connect(info)


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
        print(f"  ? {table} host={os.environ.get('POSTGRES_HOST')} "
              f"db={os.environ.get('POSTGRES_DB')} debut={debut!r} fin={fin!r}")
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
    """Extrait la fenêtre [debut, fin). Retourne les chemins écrits (0 ou 1)."""
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
