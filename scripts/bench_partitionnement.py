"""Jour 28 : octets lus selon partitionnement et clustering (ADR-033)."""

import datetime as dt
import os

from google.cloud import bigquery

PROJET = os.environ["GCP_PROJECT_ID"]
DS = os.environ["BQ_DATASET_MARTS"]
SOURCE = f"`{PROJET}.{DS}.fct_bookings`"
LABELS = {"composant": "bench"}
client = bigquery.Client(project=PROJET)

# Colonne de montant détectée dans le schéma, pour lire une colonne réaliste
schema = client.get_table(f"{PROJET}.{DS}.fct_bookings").schema
mesure = next(
    (
        f.name
        for f in schema
        if f.field_type in ("NUMERIC", "FLOAT", "BIGNUMERIC")
        and ("amount" in f.name or "montant" in f.name)
    ),
    None,
)
agg = f"SUM({mesure})" if mesure else "COUNT(*)"
print(f"colonne de mesure : {mesure or 'aucune (COUNT)'}")

# Une seule date de coupure, calculée une fois, pour les trois copies
coupure = dt.date.today() - dt.timedelta(days=55)
FILTRE = f"WHERE booking_date >= DATE '{coupure}'"

VARIANTES = {
    "bench_brut": "",
    "bench_cluster": "CLUSTER BY booking_date, hotel_id",
    "bench_part": "PARTITION BY booking_date CLUSTER BY hotel_id",
}


def executer(sql, dry=False):
    cfg = bigquery.QueryJobConfig(dry_run=dry, use_query_cache=False, labels=LABELS)
    job = client.query(sql, job_config=cfg)
    if not dry:
        job.result()
    return job


# 1. Construction des trois copies
for nom, options in VARIANTES.items():
    executer(
        f"CREATE OR REPLACE TABLE `{PROJET}.{DS}.{nom}` {options} "
        f"AS SELECT * FROM {SOURCE} {FILTRE}"
    )

# 2. Preuve que les trois copies contiennent les mêmes lignes
comptes = {
    nom: list(executer(f"SELECT COUNT(*) n FROM `{PROJET}.{DS}.{nom}`").result())[0].n
    for nom in VARIANTES
}
print("lignes :", comptes)
assert len(set(comptes.values())) == 1, "Les copies diffèrent : mesure invalide"

# Hôtel le plus fréquent, pour la requête 2
hotel = list(
    executer(
        f"SELECT hotel_id FROM `{PROJET}.{DS}.bench_brut` GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1"
    ).result()
)[0].hotel_id
hotel_sql = f"'{hotel}'" if isinstance(hotel, str) else str(hotel)

REQUETES = {
    "R1 fenêtre 7 j": "SELECT hotel_id, {agg} FROM `{t}` "
    "WHERE booking_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY 1",
    "R2 un hôtel": "SELECT booking_date, {agg} FROM `{t}` "
    f"WHERE hotel_id = {hotel_sql} GROUP BY 1",
    "R3 témoin": "SELECT DATE_TRUNC(booking_date, WEEK), {agg} FROM `{t}` GROUP BY 1",
}

# 3. Mesure : dry-run (estimation), puis exécution réelle sans cache
print("\n| Requête | Copie | Dry-run (o) | Traités (o) | Facturés (o) |")
print("|---|---|---:|---:|---:|")
for rq, modele in REQUETES.items():
    for nom in VARIANTES:
        sql = modele.format(agg=agg, t=f"{PROJET}.{DS}.{nom}")
        estim = executer(sql, dry=True).total_bytes_processed
        reel = executer(sql)
        print(
            f"| {rq} | {nom} | {estim:,} | {reel.total_bytes_processed:,} "
            f"| {reel.total_bytes_billed:,} |"
        )

# 4. Nettoyage (mettre GARDER=1 pour conserver les copies)
if os.environ.get("GARDER") != "1":
    for nom in VARIANTES:
        client.delete_table(f"{PROJET}.{DS}.{nom}", not_found_ok=True)
    print("\ncopies supprimées")
