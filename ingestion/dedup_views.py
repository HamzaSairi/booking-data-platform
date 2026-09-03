"""Vues de déduplication au-dessus de la couche raw.

La raw est un journal append-only qui contient des doublons par
construction (rejeu après crash, réinitialisation de watermark).
La déduplication est une opération de LECTURE : on ne réécrit jamais raw.

Ces vues sont remplacées par les modèles dbt stg_* au jour 17.
"""

import os

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT = os.environ["GCP_PROJECT_ID"]
RAW = "raw_booking"
STAGING = "staging_booking"
LOCATION = "EU"

TABLES = {
    "hotels": "hotel_id",
    "customers": "customer_id",
    "bookings": "booking_id",
    "payments": "payment_id",
}

# Le tri de départage compte autant que la partition.
#   updated_at DESC   -> la version métier la plus récente gagne
#   _ingested_at DESC -> départage deux copies du MÊME updated_at
# Sans le second, le gagnant serait choisi arbitrairement par BigQuery
# et la sortie ne serait pas reproductible d'une exécution à l'autre.
DDL = """
CREATE OR REPLACE VIEW `{project}.{staging}.v_{table}` AS
SELECT * EXCEPT (_ingested_at, _source_file, _rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY {pk}
      ORDER BY updated_at DESC, _ingested_at DESC
    ) AS _rn
  FROM `{project}.{raw}.{table}`
)
WHERE _rn = 1
"""


def run() -> None:
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    for table, pk in TABLES.items():
        sql = DDL.format(
            project=PROJECT, staging=STAGING, raw=RAW, table=table, pk=pk
        )
        client.query(sql).result()
        print(f"  ~ vue {STAGING}.v_{table} (clé {pk})")


if __name__ == "__main__":
    run()