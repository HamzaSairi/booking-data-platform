"""Accès BigQuery partagé par tous les scripts d'ingestion.

Point unique où le client est construit et où le plafond d'octets
facturés est appliqué. Ne jamais instancier bigquery.Client ailleurs.
"""

import os

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
if not PROJECT_ID:
    raise SystemExit("GCP_PROJECT_ID manquant — copie .env.example vers .env")

LOCATION = os.environ.get("BQ_LOCATION", "EU")
MAX_BYTES = int(os.environ.get("BQ_MAX_BYTES_BILLED", 10 * 2**30))


def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def query(sql: str, dry_run: bool = False):
    """Toute requête passe ici.

    Le plafond maximum_bytes_billed fait échouer le job AVANT exécution
    si l'estimation le dépasse : c'est le vrai garde-fou de coût, une
    alerte budget ne faisant que notifier après coup.
    """
    client = get_client()
    job_config = bigquery.QueryJobConfig(
        dry_run=dry_run,
        use_query_cache=not dry_run,
        maximum_bytes_billed=MAX_BYTES,
    )
    job = client.query(sql, job_config=job_config)
    if dry_run:
        return job.total_bytes_processed
    return job.result()


if __name__ == "__main__":
    client = get_client()
    print("Projet     :", client.project)
    print("Région     :", LOCATION)
    print("Plafond    :", f"{MAX_BYTES / 2**30:.0f} Gio par requête")
    print("Datasets   :", sorted(d.dataset_id for d in client.list_datasets()))

    octets = query("SELECT 1 AS ok", dry_run=True)
    print(f"Dry-run OK : {octets} octets seraient facturés")