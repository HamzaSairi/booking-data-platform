"""Contrôle du SCD2 avant le dashboard (jour 29)."""

import os

from google.cloud import bigquery

c = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
M, R = os.environ["BQ_DATASET_MARTS"], os.environ["BQ_DATASET_RAW"]


def show(titre, sql):
    print(f"== {titre}")
    for r in c.query(sql, location="EU").result():
        print("  ", dict(r))


show(
    "versions SCD2",
    f"""SELECT COUNT(*) versions, COUNT(DISTINCT customer_id) clients,
  COUNTIF(NOT is_current) versions_anciennes FROM `{M}.dim_customers`""",
)

show(
    "fidélité à la réservation vs actuelle",
    f"""
SELECT COUNT(*) reservations,
  COUNTIF(f.customer_sk IS NULL) sans_version,
  COUNTIF(h.loyalty_tier != c.loyalty_tier) niveau_different,
  COUNTIF(f.is_late_arriving_customer) clients_tardifs
FROM `{M}.fct_bookings` f
LEFT JOIN `{M}.dim_customers` h ON h.customer_sk = f.customer_sk
LEFT JOIN `{M}.dim_customers` c ON c.customer_id = f.customer_id AND c.is_current""",
)

show("devises", f"SELECT currency, COUNT(*) n FROM `{M}.fct_bookings` GROUP BY 1")

show(
    "partitions raw clients : première expiration",
    f"""
SELECT table_name, MIN(partition_id) premiere_partition, COUNT(*) partitions,
  DATE_ADD(PARSE_DATE('%Y%m%d', MIN(partition_id)), INTERVAL 60 DAY) expire_vers
FROM `{R}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name IN ('customers', 'customers_cdc')
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
GROUP BY 1""",
)
