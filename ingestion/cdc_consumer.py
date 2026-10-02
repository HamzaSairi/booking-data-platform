"""Consommateur CDC : topics Debezium -> BigQuery raw_booking.{table}_cdc.

Garantie at-least-once : l'offset n'est commité qu'après le succès de TOUS
les load jobs du micro-batch. Un plantage entre les deux rejoue le
micro-batch au redémarrage : doublons possibles dans la raw, à dédupliquer
en aval sur (_kafka_topic, _kafka_partition, _kafka_offset). Aucune perte.

La raw garde le message tel que Debezium l'a émis (before / after en JSON) :
typage et conversion des dates (jours depuis 1970) se font dans dbt.

Lancement, depuis la racine du dépôt :
    python -m ingestion.cdc_consumer                       # en continu
    python -m ingestion.cdc_consumer --exit-when-idle 30   # s'arrête si inactif
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, TopicPartition
from google.cloud import bigquery

from ingestion.bq import PROJECT_ID, get_client

TABLES = ["customers", "hotels", "bookings", "payments"]
TOPIC_PREFIX = "booking.public."
DATASET = os.environ.get("BQ_DATASET_RAW", "raw_booking")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
GROUP_ID = os.environ.get("CDC_GROUP_ID", "booking-cdc-bq")

SCHEMA = [
    bigquery.SchemaField("op", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("lsn", "INT64"),
    bigquery.SchemaField("tx_id", "INT64"),
    bigquery.SchemaField("source_ts", "TIMESTAMP"),
    bigquery.SchemaField("captured_at", "TIMESTAMP"),
    bigquery.SchemaField("snapshot", "STRING"),
    bigquery.SchemaField("_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("before", "JSON"),
    bigquery.SchemaField("after", "JSON"),
    bigquery.SchemaField("_kafka_topic", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_kafka_partition", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("_kafka_offset", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
]


def log(event: str, **champs) -> None:
    """Une ligne JSON par événement : lisible par un humain et par une machine."""
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    print(json.dumps({"ts": ts, "event": event, **champs}, ensure_ascii=False), flush=True)


def iso_ms(ms: int | None) -> str | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def creer_tables(client: bigquery.Client) -> None:
    for t in TABLES:
        table = bigquery.Table(f"{PROJECT_ID}.{DATASET}.{t}_cdc", schema=SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_ingested_at"
        )
        table.clustering_fields = ["_key"]
        client.create_table(table, exists_ok=True)


def vers_ligne(msg, valeur: dict) -> dict:
    src = valeur.get("source") or {}
    return {
        "op": valeur["op"],
        "lsn": src.get("lsn"),
        "tx_id": src.get("txId"),
        "source_ts": iso_ms(src.get("ts_ms")),
        "captured_at": iso_ms(valeur.get("ts_ms")),
        "snapshot": src.get("snapshot"),
        "_key": msg.key().decode("utf-8"),
        "before": valeur.get("before"),
        "after": valeur.get("after"),
        "_kafka_topic": msg.topic(),
        "_kafka_partition": msg.partition(),
        "_kafka_offset": msg.offset(),
    }


def lag_total(consumer: Consumer) -> int:
    """Messages produits mais pas encore commités, toutes partitions assignées."""
    total = 0
    for tp in consumer.committed(consumer.assignment(), timeout=10):
        bas, haut = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
        total += haut - (tp.offset if tp.offset >= 0 else bas)
    return total


def vider(client, consumer, tampon, a_commiter, tombstones, crash: bool) -> None:
    """Charge le micro-batch, PUIS commite. L'ordre est la garantie."""
    t0 = time.monotonic()
    ingested_at = datetime.now(UTC).isoformat()
    charges = {}
    for table, lignes in tampon.items():
        if not lignes:
            continue
        for ligne in lignes:
            ligne["_ingested_at"] = ingested_at
        config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_APPEND")
        client.load_table_from_json(
            lignes, f"{PROJECT_ID}.{DATASET}.{table}_cdc", job_config=config
        ).result()
        charges[table] = len(lignes)

    offsets = [TopicPartition(t, p, o) for (t, p), o in a_commiter.items()]
    if crash:
        log(
            "crash_simule",
            lignes=charges,
            offsets_non_commites={f"{tp.topic}[{tp.partition}]": tp.offset for tp in offsets},
        )
        os._exit(1)

    consumer.commit(offsets=offsets, asynchronous=False)
    log(
        "micro_batch",
        lignes=charges,
        tombstones=tombstones,
        lot=ingested_at,
        lag=lag_total(consumer),
        duree_s=round(time.monotonic() - t0, 2),
    )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--max-messages", type=int, default=1000)
    p.add_argument("--max-seconds", type=float, default=300)
    p.add_argument(
        "--exit-when-idle",
        type=float,
        default=0,
        help="s'arrêter après N secondes sans message (0 = jamais)",
    )
    p.add_argument(
        "--crash-after-load",
        action="store_true",
        help="TEST : quitter brutalement après les load jobs, avant le commit",
    )
    args = p.parse_args()

    client = get_client()
    creer_tables(client)
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC_PREFIX + t for t in TABLES])
    log(
        "demarrage",
        bootstrap=BOOTSTRAP,
        groupe=GROUP_ID,
        dataset=DATASET,
        max_messages=args.max_messages,
        max_seconds=args.max_seconds,
    )

    tampon = {t: [] for t in TABLES}
    a_commiter: dict[tuple[str, int], int] = {}
    tombstones = 0
    debut_lot = dernier_message = time.monotonic()

    try:
        while True:
            msg = consumer.poll(1.0)
            maintenant = time.monotonic()
            if msg is None:
                if args.exit_when_idle and maintenant - dernier_message >= args.exit_when_idle:
                    log("inactif", secondes=args.exit_when_idle)
                    break
            elif msg.error():
                raise RuntimeError(f"Erreur Kafka : {msg.error()}")
            else:
                dernier_message = maintenant
                if not a_commiter:
                    debut_lot = maintenant
                a_commiter[(msg.topic(), msg.partition())] = msg.offset() + 1
                if msg.value() is None:
                    tombstones += 1  # la suppression est portée par le op='d' précédent
                else:
                    table = msg.topic().removeprefix(TOPIC_PREFIX)
                    tampon[table].append(vers_ligne(msg, json.loads(msg.value())))

            en_attente = sum(len(v) for v in tampon.values())
            if a_commiter and (
                en_attente >= args.max_messages or maintenant - debut_lot >= args.max_seconds
            ):
                vider(client, consumer, tampon, a_commiter, tombstones, args.crash_after_load)
                tampon = {t: [] for t in TABLES}
                a_commiter, tombstones = {}, 0
    except KeyboardInterrupt:
        log("interruption")
    # Arrêt propre (inactivité ou Ctrl+C) : on vide ce qui reste.
    # Sur toute autre exception, on sort SANS commit : le lot sera rejoué.
    if a_commiter:
        vider(client, consumer, tampon, a_commiter, tombstones, args.crash_after_load)
    log("arret", lag=lag_total(consumer))
    consumer.close()


if __name__ == "__main__":
    main()
