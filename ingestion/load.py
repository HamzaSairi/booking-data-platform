"""Chargement des Parquet locaux vers BigQuery (raw_booking).

Idempotence par construction : chaque fenêtre écrase sa propre partition,
identifiée par la date logique `_interval_start`. Le contenu d'une
partition ne dépend que des fichiers de sa fenêtre, jamais de l'historique
des exécutions. Rejouer une fenêtre est donc sans effet de bord, et une
donnée arrivée en retard dans cette fenêtre est prise en compte au
rejeu — ce qu'un manifeste « déjà chargé » aurait refusé.

Bibliothèque autonome : aucune dépendance à Airflow. La configuration
vient de l'environnement, `.env` en local ou variables injectées par
Compose côté orchestrateur.
"""

import io
import os
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

load_dotenv()

DATASET = "raw_booking"
LOCATION = "EU"                      # doit correspondre au dataset

# Ancrées sur la racine du dépôt, jamais sur le répertoire courant :
# Airflow exécute ses tâches depuis /opt/airflow.
RACINE = Path(__file__).resolve().parents[1]
DATA_DIR = RACINE / "data" / "raw"

# Clustering sur la PK : c'est la colonne de jointure du MERGE du jour 9.
TABLES = {
    "hotels": "hotel_id",
    "customers": "customer_id",
    "bookings": "booking_id",
    "payments": "payment_id",
}


def projet() -> str:
    """Lecture paresseuse : au niveau module, une variable manquante ferait
    échouer le parse du DAG entier, pas seulement la tâche concernée."""
    try:
        return os.environ["GCP_PROJECT_ID"]
    except KeyError as exc:
        raise RuntimeError(f"Variable d'environnement manquante : {exc.args[0]}") from exc


def rel(chemin: Path) -> str:
    """Chemin relatif à la racine : le même sur l'hôte et dans le conteneur."""
    return chemin.relative_to(RACINE).as_posix()


# ─── Chargement ──────────────────────────────────────────────────────
def annoter(fichiers: list[str], ingested_at: datetime, debut: datetime) -> pa.Table | None:
    """Concatène les fichiers et ajoute les métadonnées techniques.

    `_ingested_at` est l'heure physique d'entrée : métadonnée d'audit, elle
    doit dire la vérité, et c'est le seul now() légitime du pipeline.
    `_interval_start` est la date logique, et c'est elle qui porte le
    partitionnement — deux exécutions de la même fenêtre visent la même
    partition, quel que soit le jour où on les lance.
    """
    morceaux = []
    for f in fichiers:
        t = pq.read_table(RACINE / f)
        if t.num_rows == 0:
            continue
        t = t.append_column(
            "_ingested_at",
            pa.array([ingested_at] * t.num_rows, type=pa.timestamp("us", tz="UTC")),
        )
        t = t.append_column(
            "_source_file",
            pa.array([f] * t.num_rows, type=pa.string()),
        )
        # date32 impérativement : un TIMESTAMP ferait échouer le décorateur
        # de partition avec « Incompatible partition specification ».
        t = t.append_column(
            "_interval_start",
            pa.array([debut.date()] * t.num_rows, type=pa.date32()),
        )
        morceaux.append(t)

    if not morceaux:
        return None
    # promote_options : deux fichiers peuvent différer si une colonne était
    # entièrement nulle dans l'un des lots. Sur pyarrow < 14 : promote=True.
    return pa.concat_tables(morceaux, promote_options="permissive")


def charger(client: bigquery.Client, destination: str, table: str,
            arrow: pa.Table, amorcage: bool) -> int:
    """Joue le load job. `destination` porte le décorateur de partition en
    régime normal, la table nue à l'amorçage."""
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if amorcage:
        # Partitionnement et clustering ne se déclarent qu'à la création.
        # schema_update_options est refusé ici : faire évoluer un schéma
        # suppose un schéma préexistant. Le Parquet fait foi.
        config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="_interval_start",
        )
        config.clustering_fields = [TABLES[table]]
    else:
        # Une colonne ajoutée en source ne doit pas faire échouer le
        # pipeline. Une colonne supprimée, si — c'est la question 8.
        config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]

    buf = io.BytesIO()
    pq.write_table(arrow, buf, compression="snappy")
    buf.seek(0)                      # sans quoi BigQuery lirait zéro octet

    job = client.load_table_from_file(buf, destination, job_config=config, location=LOCATION)
    job.result()                     # lève l'exception si le job échoue
    # output_rows n'est pas garanti sur un WRITE_TRUNCATE avec décorateur
    # de partition ; le compte du Parquet fait foi puisque le job a réussi.
    return job.output_rows if job.output_rows is not None else arrow.num_rows


def load_one(table: str, fichiers: list[str], debut: datetime) -> int:
    """Écrase la partition correspondant à la fenêtre."""
    if not fichiers:
        return 0

    arrow = annoter(fichiers, datetime.now(UTC), debut)
    if arrow is None:
        return 0

    client = bigquery.Client(project=projet(), location=LOCATION)
    ref = f"{projet()}.{DATASET}.{table}"

    try:
        client.get_table(ref)
        amorcage = False
        # Le décorateur restreint l'écrasement à une seule partition, et
        # BigQuery refuse le job si une ligne tombe en dehors : garde-fou
        # gratuit contre une erreur de bornes dans l'extracteur.
        destination = f"{ref}${debut:%Y%m%d}"
    except NotFound:
        amorcage = True
        destination = ref

    lignes = charger(client, destination, table, arrow, amorcage)
    print(f"  + {table} partition {debut:%F} : {lignes} lignes")
    return lignes


# ─── Point d'entrée CLI ──────────────────────────────────────────────
def run(debut: datetime, fin: datetime) -> None:
    from ingestion.extract import chemin_partition

    for table in TABLES:
        chemin = chemin_partition(table, debut)
        load_one(table, [rel(chemin)] if chemin.exists() else [], debut)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Chargement d'une fenêtre temporelle.")
    p.add_argument("--debut", required=True, help="ISO 8601, ex. 2026-09-03T00:00:00+00:00")
    p.add_argument("--fin", required=True)
    a = p.parse_args()
    run(datetime.fromisoformat(a.debut), datetime.fromisoformat(a.fin))