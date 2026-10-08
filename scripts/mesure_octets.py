"""Octets estimés / traités / facturés : fct_bookings (partitionnée par
booking_date, clusterisée par hotel_id) contre une copie brute.

Protocole : docs/optimisation-bigquery.md. Exécution réelle sans cache,
sinon BigQuery renvoie 0 octet. Jobs étiquetés composant=mesure.
"""

import os
from datetime import timedelta

from google.cloud import bigquery

PROJET = os.environ["GCP_PROJECT_ID"]
MARTS = os.environ.get("BQ_DATASET_MARTS", "marts_booking")
OPTI = f"`{PROJET}.{MARTS}.fct_bookings`"
BRUT = f"`{PROJET}.{MARTS}.fct_bookings_brut`"
LABELS = {"composant": "mesure"}

client = bigquery.Client(project=PROJET, location="EU")


def executer(sql, **options):
    config = bigquery.QueryJobConfig(labels=LABELS, use_query_cache=False, **options)
    return client.query(sql, job_config=config)


def mesurer(sql):
    estime = executer(sql, dry_run=True).total_bytes_processed
    job = executer(sql)
    job.result()
    return estime, job.total_bytes_processed, job.total_bytes_billed


def main():
    executer(f"CREATE OR REPLACE TABLE {BRUT} AS SELECT * FROM {OPTI}").result()
    try:
        ligne = next(
            iter(
                executer(
                    f"SELECT MAX(booking_date) AS fin, ANY_VALUE(hotel_id) AS hotel FROM {OPTI}"
                ).result()
            )
        )
        fin, hotel = ligne.fin, ligne.hotel
        debut = fin - timedelta(days=7)
        # repr() : 12 pour un entier, 'abc' pour une chaîne, valide en SQL BigQuery.
        requetes = {
            "Q1 filtre date (7 j)": (
                "SELECT COUNT(DISTINCT booking_id) FROM {t} "
                f"WHERE booking_date BETWEEN DATE '{debut}' AND DATE '{fin}'"
            ),
            "Q2 filtre hôtel": (
                f"SELECT COUNT(DISTINCT booking_id) FROM {{t}} WHERE hotel_id = {hotel!r}"
            ),
            "Q3 date + group by hôtel": (
                "SELECT hotel_id, COUNT(DISTINCT customer_id) FROM {t} "
                f"WHERE booking_date >= DATE '{debut}' GROUP BY hotel_id"
            ),
        }
        print(f"Mesure du {fin} (fenêtre {debut} → {fin}, hôtel {hotel!r})\n")
        print("| Requête | Table | Estimé (dry-run) | Traité | Facturé |")
        print("|---|---|---:|---:|---:|")
        for nom, gabarit in requetes.items():
            for libelle, table in (("brute", BRUT), ("partitionnée + clusterisée", OPTI)):
                estime, traite, facture = mesurer(gabarit.format(t=table))
                print(f"| {nom} | {libelle} | {estime:,} | {traite:,} | {facture:,} |")
    finally:
        executer(f"DROP TABLE IF EXISTS {BRUT}").result()


if __name__ == "__main__":
    main()
