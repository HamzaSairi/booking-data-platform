"""Charge le journal JSONL des callbacks dans ops_booking.pipeline_metrics.

Même contrat temporel que ingestion_batch : fenêtre [logical_date, +1 jour),
run déclenché à la fin de la journée, horloge utilisée pour refuser,
jamais pour calculer (ADR-028).

Les événements sont datés à l'OBSERVATION (horodatage du callback), pas à
la date logique du run observé : un backfill exécuté aujourd'hui sur
septembre produit des événements d'aujourd'hui, chargés demain.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.interval import CronDataIntervalTimetable

from commun.callbacks import sur_echec, sur_relance

FENETRE = timedelta(days=1)


@dag(
    dag_id="pipeline_metrics",
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_retry_callback": sur_relance,
        "on_failure_callback": sur_echec,
        # Pas de on_success_callback : ce DAG chargerait ses propres
        # succès au run suivant et se mesurerait lui-même.
    },
    tags=["observabilite"],
    doc_md=__doc__,
)
def pipeline_metrics():
    @task
    def charger_metriques() -> int:
        from commun.metriques import charger

        debut = get_current_context().get("logical_date")
        if debut is None:
            raise AirflowFailException(
                "Run sans logical_date : fenêtre indéfinie. "
                "Déclencher avec --logical-date AAAA-MM-JJT00:00:00+00:00."
            )
        fin = debut + FENETRE
        if fin > pendulum.now("UTC"):
            raise AirflowFailException(f"Fenêtre [{debut}, {fin}) non close.")
        return charger(debut, fin)

    charger_metriques()


pipeline_metrics()
