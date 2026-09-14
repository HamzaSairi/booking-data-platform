"""Tests de l'extraction par fenetre (reecrits au jour 15).

Version d'origine (jour 7) : le watermark imposait une marge de securite,
donc un chevauchement tolere entre deux extractions. Depuis le jour 13,
l'extraction est une fonction pure de sa fenetre (ADR-019) : le
chevauchement est nul et l'egalite devient stricte. Un meilleur design
permet un test plus fort.
"""

from datetime import UTC, datetime, timedelta

import pyarrow.parquet as pq
import pytest

from ingestion.extract import chemin_partition, extract_one

TABLES = ["hotels", "customers", "bookings", "payments"]

# Fenetre d'hier : close, donc son contenu ne bouge plus pendant le test.
DEBUT = (datetime.now(UTC) - timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0)
FIN = DEBUT + timedelta(days=1)


def lignes_parquet(table: str) -> int:
    """Lignes du Parquet de la fenetre, 0 s'il n'existe pas."""
    chemin = chemin_partition(table, DEBUT)
    return pq.read_metadata(chemin).num_rows if chemin.exists() else 0


@pytest.mark.parametrize("table", TABLES)
def test_extraction_idempotente(table):
    """Rejouer une fenetre reecrit le meme fichier, sans doublon.

    C'est la propriete qui rend un backfill sur : le resultat ne depend
    que de la fenetre, jamais du nombre d'executions.
    """
    extract_one(table, DEBUT, FIN)
    premier = lignes_parquet(table)

    extract_one(table, DEBUT, FIN)
    assert lignes_parquet(table) == premier, (
        f"{table} : {lignes_parquet(table)} lignes au second passage contre "
        f"{premier} au premier. L'extraction n'est pas idempotente."
    )


@pytest.mark.parametrize("table", TABLES)
def test_extraction_complete(table, cur):
    """Le Parquet contient exactement les lignes de la fenetre en source."""
    extract_one(table, DEBUT, FIN)

    cur.execute(
        f"SELECT count(*) FROM {table} "
        "WHERE updated_at >= %(debut)s AND updated_at < %(fin)s",
        {"debut": DEBUT, "fin": FIN},
    )
    n_source = cur.fetchone()[0]

    assert lignes_parquet(table) == n_source, (
        f"{table} : {lignes_parquet(table)} lignes extraites pour {n_source} "
        f"en source sur [{DEBUT:%F}, {FIN:%F})."
    )
