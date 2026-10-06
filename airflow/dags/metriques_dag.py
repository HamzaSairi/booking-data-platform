"""Charge les événements des callbacks (JSONL) dans BigQuery : pipeline_metrics.

Une exécution = une journée UTC = une partition, écrasée (WRITE_TRUNCATE).
Rejouer ou backfiller ne crée aucun doublon (même logique que l'ADR-020).
Load job et non streaming : le sandbox refuse insert_rows_json.
Pas de clustering : la mesure du jour 28 montre qu'il est nul à ce volume.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

from commun.callbacks import fichier_evenements, sur_echec, sur_relance, sur_succes

log = logging.getLogger(__name__)

# Schéma explicite : un jour sans échec ne contient que des exception_* nuls,
# et l'autodétection ne créerait pas ces colonnes.
CHAMPS = [
    ("horodatage", "TIMESTAMP", "REQUIRED"),
    ("evenement", "STRING", "REQUIRED"),
    ("dag_id", "STRING", "NULLABLE"),
    ("task_id", "STRING", "NULLABLE"),
    ("run_id", "STRING", "NULLABLE"),
    ("logical_date", "TIMESTAMP", "NULLABLE"),
    ("tentative", "INT64", "NULLABLE"),
    ("retries", "INT64", "NULLABLE"),
    ("relances_max", "INT64", "NULLABLE"),
    ("duree_tentative_s", "FLOAT64", "NULLABLE"),
    ("exception_type", "STRING", "NULLABLE"),
    ("exception_message", "STRING", "NULLABLE"),
    ("scenario_runbook", "STRING", "NULLABLE"),
    ("_charge_le", "TIMESTAMP", "REQUIRED"),
]


@dag(
    dag_id="metriques_dag",
    # Explicite : en Airflow 3, "@daily" donne des intervalles de durée nulle,
    # qui feraient supprimer la partition du jour (bug du jour 28).
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 9, 10, tzinfo=UTC),  # création du fichier JSONL
    catchup=True,
    max_active_runs=3,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_retry_callback": sur_relance,
        "on_failure_callback": sur_echec,
        "on_success_callback": sur_succes,
    },
    tags=["observabilite"],
)
def metriques_dag():
    @task
    def charger(data_interval_start=None, data_interval_end=None) -> int:
        from google.cloud import bigquery

        debut, fin = data_interval_start, data_interval_end

        # Garde-fou : ce code supprime une partition quand la journée est vide.
        # Un intervalle anormal doit échouer bruyamment, pas passer pour un jour calme.
        if debut is None or fin is None or fin - debut != timedelta(days=1):
            raise ValueError(f"Intervalle inattendu {debut} → {fin} : partition non modifiée")

        # Lues avant toute action : une variable manquante échoue sans effet de bord.
        projet = os.environ["GCP_PROJECT_ID"]
        dataset = os.environ["BQ_DATASET_RAW"]

        charge_le = datetime.now(UTC).isoformat()
        lignes, rejetees = [], 0
        chemin = fichier_evenements()
        if chemin.exists():
            with chemin.open(encoding="utf-8") as f:
                for brut in f:
                    try:
                        ev = json.loads(brut)
                        h = datetime.fromisoformat(ev["horodatage"])
                    except (json.JSONDecodeError, KeyError, ValueError):
                        rejetees += 1  # ligne tronquée par un arrêt brutal
                        continue
                    if debut <= h < fin:
                        ev["_charge_le"] = charge_le
                        lignes.append(ev)
        log.info("%s → %s : %d événements, %d lignes rejetées", debut, fin, len(lignes), rejetees)

        client = bigquery.Client(project=projet)
        table_id = f"{projet}.{dataset}.pipeline_metrics"
        schema = [bigquery.SchemaField(n, t, mode=m) for n, t, m in CHAMPS]

        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="horodatage"
        )
        client.create_table(table, exists_ok=True)

        partition = f"{table_id}${debut:%Y%m%d}"
        if not lignes:
            # Journée vide : on supprime une éventuelle partition d'un run
            # précédent, sinon un rejeu laisserait des lignes périmées.
            client.delete_table(partition, not_found_ok=True)
            return 0

        cfg = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ignore_unknown_values=True,  # un champ ajouté demain ne casse rien
            labels={"composant": "metriques"},
        )
        client.load_table_from_json(lignes, partition, job_config=cfg).result()
        return len(lignes)

    charger()


metriques_dag()
