"""Chargement des Parquet locaux vers BigQuery (raw_booking).

Garantie : at-least-once. Le job de chargement est joué AVANT la mise à
jour du manifeste. Un plantage entre les deux rejoue le fichier au tour
suivant (doublon tracé par _source_file, dédupliqué au jour 9) ;
l'ordre inverse le perdrait.
"""

import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

try:
    PROJECT = os.environ["GCP_PROJECT_ID"]
except KeyError as exc:
    raise SystemExit(f"Variable manquante dans .env : {exc.args[0]}") from exc
DATASET = "raw_booking"
LOCATION = "EU"                      # doit correspondre au dataset, cf. plus bas

DATA_DIR = Path("data/raw")
MANIFEST = Path("state/loaded_files.json")

# Clustering sur la PK : c'est la colonne de jointure du MERGE du jour 9.
TABLES = {
    "hotels": "hotel_id",
    "customers": "customer_id",
    "bookings": "booking_id",
    "payments": "payment_id",
}


def charger_manifeste() -> set[str]:
    if not MANIFEST.exists():
        return set()
    return set(json.loads(MANIFEST.read_text(encoding="utf-8")))


def sauver_manifeste(fichiers: set[str]) -> None:
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    tmp = MANIFEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(fichiers), indent=2), encoding="utf-8")
    tmp.replace(MANIFEST)            # écriture atomique, cf. jour 7


def fichiers_a_charger(table: str, deja: set[str]) -> list[Path]:
    dossier = DATA_DIR / table
    if not dossier.exists():
        return []
    return sorted(f for f in dossier.rglob("*.parquet") if str(f) not in deja)


def annoter(fichiers: list[Path], ingested_at: datetime) -> pa.Table | None:
    """Concatène les fichiers et ajoute les métadonnées techniques."""
    morceaux = []
    for f in fichiers:
        t = pq.read_table(f)
        if t.num_rows == 0:
            continue
        t = t.append_column(
            "_ingested_at",
            pa.array([ingested_at] * t.num_rows, type=pa.timestamp("us", tz="UTC")),
        )
        t = t.append_column(
            "_source_file",
            pa.array([str(f)] * t.num_rows, type=pa.string()),
        )
        morceaux.append(t)

    if not morceaux:
        return None
    # promote_options : deux fichiers peuvent différer si une colonne était
    # entièrement nulle dans l'un des lots. Sur pyarrow < 14 : promote=True.
    return pa.concat_tables(morceaux, promote_options="permissive")


def charger(client: bigquery.Client, table: str, arrow: pa.Table) -> int:
    ref = f"{PROJECT}.{DATASET}.{table}"

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


def run() -> None:
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    ingested_at = datetime.now(timezone.utc)   # figé pour tout le lot
    deja = charger_manifeste()

    for table in TABLES:
        fichiers = fichiers_a_charger(table, deja)
        if not fichiers:
            print(f"  = {table}: rien à charger")
            continue

        arrow = annoter(fichiers, ingested_at)
        if arrow is None:
            deja.update(str(f) for f in fichiers)
            continue

        lignes = charger(client, table, arrow)
        print(f"  + {table}: {lignes} lignes depuis {len(fichiers)} fichier(s)")

        # Job réussi -> seulement maintenant on marque les fichiers.
        deja.update(str(f) for f in fichiers)
        sauver_manifeste(deja)

    print(f"Lot {ingested_at.isoformat()}")


if __name__ == "__main__":
    run()