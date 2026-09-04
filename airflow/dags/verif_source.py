"""Vérifie que l'orchestrateur sait joindre la base métier.

Ce DAG n'a aucun effet de bord : il lit un compte et l'affiche. Il ne sert
qu'à valider la chaîne Airflow → Connection → Postgres source avant que le
jour 12 n'y branche l'ingestion réelle.
"""

from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


@dag(
    dag_id="verif_source",
    schedule=None,             # déclenchement manuel : la planification, c'est le jour 12
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    tags=["socle"],
    doc_md=__doc__,
)
def verif_source():
    @task
    def compter_bookings() -> int:
        # Le hook est construit DANS la tâche : au niveau module, il serait
        # instancié à chaque parse du fichier, soit deux fois par minute.
        hook = PostgresHook(postgres_conn_id="postgres_source")
        (total,) = hook.get_first("SELECT count(*) FROM bookings")
        print(f"bookings en source : {total}")
        return total

    compter_bookings()


verif_source()