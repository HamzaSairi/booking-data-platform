"""Tests de l'extraction incrémentale (Jour 7)."""

import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

DATA_DIR = Path("data/raw")
STATE_FILE = Path("state/watermarks.json")
TABLES = ["hotels", "customers", "bookings", "payments"]


def lancer_extraction() -> None:
    r = subprocess.run(
        [sys.executable, "ingestion/extract.py"],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, r.stderr


def lignes_par_table() -> dict[str, int]:
    """Total de lignes présentes dans les Parquet, par table."""
    totaux = {}
    for table in TABLES:
        fichiers = list((DATA_DIR / table).rglob("*.parquet"))
        totaux[table] = sum(pq.read_metadata(f).num_rows for f in fichiers)
    return totaux


def test_seconde_extraction_borne_la_redondance(cur):
    """At-least-once : un chevauchement borné est acceptable, une perte non.

    La marge de sécurité (SAFETY_MARGIN) fait délibérément relire les lignes
    proches du watermark. On vérifie que ce chevauchement reste marginal,
    pas qu'il est nul — un seuil à zéro interdirait la marge.
    """
    lancer_extraction()
    avant = lignes_par_table()

    lancer_extraction()
    apres = lignes_par_table()

    for table in TABLES:
        relues = apres[table] - avant[table]
        assert relues >= 0
        # Seuil : 5 % de la table, ou 50 lignes pour les petites tables.
        plafond = max(50, int(avant[table] * 0.05))
        assert relues <= plafond, (
            f"{table} : {relues} lignes relues, au-delà du chevauchement "
            f"attendu ({plafond}). Le watermark n'avance probablement pas."
        )


def test_aucune_perte(cur):
    """Toute ligne de la source doit exister dans au moins un Parquet."""
    lancer_extraction()
    parquet = lignes_par_table()

    for table in TABLES:
        n_source = cur.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        assert parquet[table] >= n_source, (
            f"{table} : {parquet[table]} lignes extraites pour {n_source} en "
            f"source. Des lignes ont été perdues."
        )


def test_watermark_jamais_dans_le_futur():
    """Un updated_at aberrant ne doit pas bloquer l'avancement du pipeline."""
    from datetime import datetime, timezone
    import json

    lancer_extraction()
    etat = json.loads(STATE_FILE.read_text())
    maintenant = datetime.now(timezone.utc)

    for table, valeur in etat["watermarks"].items():
        wm = datetime.fromisoformat(valeur)
        assert wm <= maintenant, f"{table} : watermark dans le futur ({wm})"