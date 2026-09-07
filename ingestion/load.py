"""Chargement des Parquet locaux vers BigQuery (raw_booking).

Garantie : at-least-once. Le job de chargement est joué AVANT la mise à
jour du manifeste. Un plantage entre les deux rejoue le fichier au tour
suivant (doublon tracé par _source_file, dédupliqué au jour 17) ;
l'ordre inverse le perdrait.

Bibliothèque autonome : aucune dépendance à Airflow. La configuration
vient de l'environnement, `.env` en local ou variables injectées par
Compose côté orchestrateur.
"""

import io
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

DATASET = "raw_booking"
LOCATION = "EU"                      # doit correspondre au dataset

# Ancrées sur la racine du dépôt, jamais sur le répertoire courant :
# Airflow exécute ses tâches depuis /opt/airflow.
RACINE = Path(__file__).resolve().parents[1]
DATA_DIR = RACINE / "data" / "raw"
STATE_DIR = RACINE / "state" / "loaded"

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


# ─── Manifeste, un fichier par table ─────────────────────────────────
# Un fichier unique serait écrasé par le dernier des quatre loads lancés
# en parallèle par le DAG. On partitionne au lieu de verrouiller.
def fichier_manifeste(table: str) -> Path:
    return STATE_DIR / f"{table}.json"


def charger_manifeste(table: str) -> set[str]:
    f = fichier_manifeste(table)
    return set(json.loads(f.read_text(encoding="utf-8"))) if f.exists() else set()


def sauver_manifeste(table: str, fichiers: set[str]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    cible = fichier_manifeste(table)
    tmp = cible.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(fichiers), indent=2), encoding="utf-8")
    tmp.replace(cible)               # écriture atomique, cf. jour 7


def fichiers_a_charger(table: str, deja: set[str]) -> list[str]:
    dossier = DATA_DIR / table
    if not dossier.exists():
        return []
    return sorted(rel(f) for f in dossier.rglob("*.parquet") if rel(f) not in deja)


# ─── Chargement ──────────────────────────────────────────────────────
def annoter(fichiers: list[str], ingested_at: datetime) -> pa.Table | None:
    """Concatène les fichiers et ajoute les métadonnées techniques."""
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
        morceaux.append(t)

    if not morceaux:
        return None
    # promote_options : deux fichiers peuvent différer si une colonne était
    # entièrement nulle dans l'un des lots. Sur pyarrow < 14 : promote=True.
    return pa.concat_tables(morceaux, promote_options="permissive")


def charger(client: bigquery.Client, table: str, arrow: pa.Table) -> int:
    ref = f"{projet()}.{DATASET}.{table}"

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="_ingested_at",
        ),
        clustering_fields=[TABLES[table]],
        # Une colonne ajoutée en source ne doit pas faire échouer le pipeline.
        # Une colonne supprimée, si — c'est le sujet de la question 8.
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    buf = io.BytesIO()
    pq.write_table(arrow, buf, compression="snappy")
    buf.seek(0)

    job = client.load_table_from_file(buf, ref, job_config=config, location=LOCATION)
    job.result()                     # lève l'exception si le job échoue
    return job.output_rows


def load_one(table: str, fichiers: list[str]) -> int:
    """Charge une liste explicite de fichiers. Le manifeste filtre ce qui
    est déjà passé : c'est ce qui rend la tâche rejouable sans doublon."""
    deja = charger_manifeste(table)
    restants = [f for f in fichiers if f not in deja]
    if not restants:
        print(f"  = {table} : {len(fichiers)} fichier(s) déjà chargé(s)")
        return 0

    arrow = annoter(restants, datetime.now(UTC))
    if arrow is None:                # fichiers vides : rien à charger
        deja.update(restants)
        sauver_manifeste(table, deja)
        return 0

    client = bigquery.Client(project=projet(), location=LOCATION)
    lignes = charger(client, table, arrow)

    # Job réussi -> seulement maintenant on marque les fichiers.
    deja.update(restants)
    sauver_manifeste(table, deja)

    print(f"  + {table} : {lignes} lignes depuis {len(restants)} fichier(s)")
    return lignes


# ─── Point d'entrée CLI ──────────────────────────────────────────────
def run() -> None:
    for table in TABLES:
        load_one(table, fichiers_a_charger(table, charger_manifeste(table)))


if __name__ == "__main__":
    run()