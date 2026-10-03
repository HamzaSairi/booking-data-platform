"""Réconciliation source / cible du CDC (jour 25, question 7).

Les réservations présentes dans Postgres doivent être exactement celles de
fct_bookings : ni plus (fantômes), ni moins (pertes). On compare des
identifiants, pas des volumes : un fantôme et une perte ne se compensent pas.

Préalable : simulateur arrêté, consommateur CDC à jour (lag 0), puis
`dbt build --select stg_bookings_cdc+`.
"""

import os

import psycopg
import pytest
from dotenv import load_dotenv

from ingestion.bq import PROJECT_ID, query

load_dotenv()
pytestmark = pytest.mark.bigquery


def _ids_source() -> set[int]:
    with psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    ) as conn:
        return {r[0] for r in conn.execute("SELECT booking_id FROM bookings")}


def _ids_cible() -> set[int]:
    ds = os.environ.get("BQ_DATASET_MARTS", "marts_booking")
    sql = f"SELECT booking_id FROM `{PROJECT_ID}.{ds}.fct_bookings`"
    return {r.booking_id for r in query(sql)}


def test_reservations_source_egales_cible():
    source, cible = _ids_source(), _ids_cible()
    fantomes, manquantes = sorted(cible - source), sorted(source - cible)
    assert not fantomes and not manquantes, (
        f"{len(fantomes)} fantôme(s) en cible {fantomes[:10]}, "
        f"{len(manquantes)} manquante(s) {manquantes[:10]} "
        f"(source {len(source)}, cible {len(cible)})"
    )
