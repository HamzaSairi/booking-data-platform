"""Charge les événements de callbacks d'une fenêtre dans pipeline_metrics.

Idempotent : la partition du jour est remplacée (WRITE_TRUNCATE), jamais
complétée. Load job et non insert_rows_json : le sandbox BigQuery refuse
le streaming (ADR-012).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from google.cloud import bigquery

from commun.callbacks import fichier_evenements

log = logging.getLogger(__name__)

SCHEMA = [
    bigquery.SchemaField("horodatage", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("evenement", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("dag_id", "STRING"),
    bigquery.SchemaField("task_id", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("logical_date", "TIMESTAMP"),
    bigquery.SchemaField("tentative", "INT64"),
    bigquery.SchemaField("retries", "INT64"),
    bigquery.SchemaField("relances_max", "INT64"),
    bigquery.SchemaField("duree_tentative_s", "FLOAT64"),
    bigquery.SchemaField("exception_type", "STRING"),
    bigquery.SchemaField("exception_message", "STRING"),
    bigquery.SchemaField("scenario_runbook", "STRING"),
]


def lire_evenements(debut: datetime, fin: datetime) -> list[dict]:
    """Événements dont l'horodatage tombe dans [debut, fin)."""
    chemin = fichier_evenements()
    if not chemin.exists():
        return []
    retenus = []
    with chemin.open(encoding="utf-8") as f:
        for ligne in f:
            evt = json.loads(ligne)
            if debut <= datetime.fromisoformat(evt["horodatage"]) < fin:
                retenus.append(evt)
    return retenus


def charger(debut: datetime, fin: datetime) -> int:
    evenements = lire_evenements(debut, fin)
    if not evenements:
        # Limite connue : une partition déjà chargée n'est pas vidée si
        # tous ses événements disparaissent du fichier source.
        log.info("Aucun événement entre %s et %s", debut, fin)
        return 0

    projet = os.environ["GCP_PROJECT_ID"]
    cible = f"{projet}.ops_booking.pipeline_metrics${debut:%Y%m%d}"
    config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="horodatage"),
        # Un champ ajouté plus tard aux callbacks ne doit pas casser le chargement.
        ignore_unknown_values=True,
        labels={"composant": "observabilite"},
    )
    client = bigquery.Client(project=projet, location="EU")
    client.load_table_from_json(evenements, cible, job_config=config).result()
    log.info("%d événements chargés dans %s", len(evenements), cible)
    return len(evenements)
