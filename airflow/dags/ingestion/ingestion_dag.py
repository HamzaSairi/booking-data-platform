"""Ingestion batch incrémentale Postgres -> BigQuery.

Un TaskGroup par table, les quatre en parallèle : extract -> validate -> load.
Le DAG n'implémente rien, il appelle la bibliothèque `ingestion/` et se
charge du quand, du combien de fois, et du quoi en cas d'échec.

Chaque run traite la fenêtre [data_interval_start, data_interval_end).
Aucune tâche ne consulte l'heure courante : c'est la condition du backfill.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import dag, get_current_context, task, task_group

# Clé primaire de chaque table, utilisée par les contrôles de validation.
CLES = {
    "hotels": "hotel_id",
    "customers": "customer_id",
    "bookings": "booking_id",
    "payments": "payment_id",
}

DEFAUTS = {
    # 3 relances, délai doublant à chaque échec : 30 s, 1 min, 2 min.
    # Dimensionné pour l'indisponibilité passagère (base qui redémarre,
    # quota BigQuery momentané), pas pour un bug de code.
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="ingestion_batch",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=True,           # les intervalles manquants sont rattrapés
    # Les partitions étant indépendantes, deux runs concurrents ne se
    # corrompent plus. On reste à 1 pour ménager Postgres et les quotas
    # de load jobs pendant un backfill : c'est une limite de débit,
    # plus une limite de correction.
    max_active_runs=1,
    default_args=DEFAUTS,
    tags=["ingestion", "batch"],
    doc_md=__doc__,
)
def ingestion_batch():

    @task_group
    def ingerer(table: str):
        """extract -> validate -> load pour une table."""

        @task(task_id="extract")
        def extract(table: str) -> list[str]:
            # Import DANS la tâche : au niveau module, il serait rejoué à
            # chaque parse du fichier par le dag-processor.
            from ingestion.extract import extract_one

            ctx = get_current_context()
            return extract_one(
                table, ctx["data_interval_start"], ctx["data_interval_end"]
            )

        @task(task_id="validate")
        def validate(table: str, fichiers: list[str]) -> list[str]:
            """Contrôle les fichiers avant qu'ils n'atteignent BigQuery.

            Toute anomalie lève AirflowFailException : une donnée invalide
            le restera aux trois relances. Les retries servent l'aléa
            réseau, pas la qualité.
            """
            from datetime import UTC

            import pyarrow.parquet as pq
            from ingestion.extract import RACINE

            ctx = get_current_context()

            # Levier de test manuel : déclencher le DAG avec la conf
            # {"echec_validate": "bookings"} pour vérifier que load ne part pas.
            conf = ctx["dag_run"].conf or {}
            if conf.get("echec_validate") == table:
                raise AirflowFailException(f"échec de validation simulé sur {table}")

            if not fichiers:
                print(f"{table} : rien à valider")
                return []

            debut = ctx["data_interval_start"]
            fin = ctx["data_interval_end"]

            def aware(d):
                # Un timestamp Postgres sans fuseau ressort naïf de pyarrow
                # et ne se compare pas à une date Airflow, qui est toujours
                # localisée. On normalise au lieu de laisser passer un
                # TypeError au milieu d'un backfill.
                return d if d.tzinfo else d.replace(tzinfo=UTC)

            cle = CLES[table]
            for f in fichiers:
                t = pq.read_table(RACINE / f)
                colonnes = set(t.column_names)

                manquantes = {cle, "updated_at"} - colonnes
                if manquantes:
                    raise AirflowFailException(f"{f} : colonnes absentes {manquantes}")
                if t.num_rows == 0:
                    raise AirflowFailException(f"{f} : fichier vide")

                cles = t.column(cle).to_pylist()
                if any(v is None for v in cles):
                    raise AirflowFailException(f"{f} : clé primaire nulle")
                if len(set(cles)) != len(cles):
                    raise AirflowFailException(f"{f} : {cle} en doublon dans le lot")

                # Post-condition sur l'extracteur, pas jugement sur la source :
                # une ligne hors bornes signale une clause WHERE fautive.
                bornes = [aware(d) for d in t.column("updated_at").to_pylist()]
                if min(bornes) < debut or max(bornes) >= fin:
                    raise AirflowFailException(
                        f"{f} : updated_at hors de la fenêtre [{debut}, {fin})"
                    )

                print(f"{table} : {t.num_rows} lignes validées dans {f}")

            return fichiers

        @task(task_id="load")
        def load(table: str, fichiers: list[str]) -> int:
            from ingestion.load import load_one

            # Fenêtre vide -> la partition n'est pas écrasée. Sans conséquence
            # tant que les données ne font que s'ajouter ; devient un état
            # périmé le jour où des lignes disparaissent en source.
            # Voir ADR-013, limite connue, résolue au sprint 5.
            if not fichiers:
                raise AirflowSkipException(f"{table} : aucun fichier à charger")

            ctx = get_current_context()
            return load_one(table, fichiers, ctx["data_interval_start"])

        load(table, validate(table, extract(table)))

    for table in CLES:
        ingerer.override(group_id=table)(table)


ingestion_batch()