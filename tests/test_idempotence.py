"""Idempotence du pipeline d'ingestion sous rejeu forcé.

Scénario simulé : le processus meurt entre l'écriture des données et la
mise à jour de l'état (watermark / manifeste). Au redémarrage il rejoue
un travail déjà fait. La raw accumule alors des doublons — attendu et
assumé (at-least-once) — mais la couche staging doit être inchangée.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

pytestmark = pytest.mark.bigquery

PROJECT = os.environ["GCP_PROJECT_ID"]
TABLES = ["hotels", "customers", "bookings", "payments"]
CLES = {
    "hotels": "hotel_id",
    "customers": "customer_id",
    "bookings": "booking_id",
    "payments": "payment_id",
}
FICHIERS_ETAT = [Path("state/watermarks.json"), Path("state/loaded_files.json")]
REJEUX = 2


@pytest.fixture(scope="module")
def client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT, location="EU")


def _executer_pipeline() -> None:
    for script in ("ingestion/extract.py", "ingestion/load.py"):
        subprocess.run([sys.executable, script], check=True, capture_output=True)


def _effacer_etat() -> None:
    """Simule la perte de l'état après un travail effectué."""
    for f in FICHIERS_ETAT:
        f.unlink(missing_ok=True)


def _empreinte_staging(client: bigquery.Client, table: str) -> tuple[int, str]:
    """Compte + empreinte du CONTENU de la vue dédupliquée.

    Le tri porte sur la ligne sérialisée : l'empreinte ne dépend donc ni de
    l'ordre de retour de BigQuery ni du nom de la clé primaire.
    """
    sql = f"""
    SELECT
      COUNT(*) AS lignes,
      IFNULL(TO_HEX(MD5(STRING_AGG(j, '\\n' ORDER BY j))), 'vide') AS empreinte
    FROM (
      SELECT TO_JSON_STRING(t) AS j
      FROM `{PROJECT}.staging_booking.v_{table}` AS t
    )
    """
    ligne = next(iter(client.query(sql).result()))
    return ligne.lignes, ligne.empreinte


def _lignes_raw(client: bigquery.Client, table: str) -> int:
    sql = f"SELECT COUNT(*) AS n FROM `{PROJECT}.raw_booking.{table}`"
    return next(iter(client.query(sql).result())).n


def test_le_rejeu_ne_modifie_pas_la_couche_staging(client):
    _executer_pipeline()  # on part d'un état à jour

    reference = {t: _empreinte_staging(client, t) for t in TABLES}
    raw_avant = {t: _lignes_raw(client, t) for t in TABLES}

    for _ in range(REJEUX):
        _effacer_etat()
        _executer_pipeline()

    apres = {t: _empreinte_staging(client, t) for t in TABLES}
    raw_apres = {t: _lignes_raw(client, t) for t in TABLES}

    # Garde-fou : sans ceci, le test passerait alors qu'il n'a rien rejoué.
    assert any(raw_apres[t] > raw_avant[t] for t in TABLES), (
        "aucun doublon créé : le rejeu n'a pas eu lieu, le test est creux"
    )

    for t in TABLES:
        assert apres[t] == reference[t], (
            f"{t} : sortie instable sous rejeu {reference[t]} -> {apres[t]}"
        )


def test_la_vue_ne_contient_aucune_cle_dupliquee(client):
    for table, pk in CLES.items():
        sql = f"""
        SELECT COUNT(*) AS n
        FROM (
          SELECT {pk}
          FROM `{PROJECT}.staging_booking.v_{table}`
          GROUP BY 1 HAVING COUNT(*) > 1
        )
        """
        assert next(iter(client.query(sql).result())).n == 0, f"{table} : clé dupliquée"