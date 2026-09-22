"""Réconciliation source (Postgres) -> raw (BigQuery) -> staging.

Vérifie, pour chaque table : partitions égales aux lignes source de leur
fenêtre, une seule version par clé, aucun décalage de timestamp (même
instance de source, ADR-032). Puis deux contrôles staging.
Usage : python sql/checks/reconciliation_raw.py   (code de sortie 1 si écart)
Hypothèse : aucune ligne source modifiée pendant l'exécution.
"""
import datetime as dt
import os
import subprocess
import sys
from collections import Counter

from google.cloud import bigquery

START = "2026-09-01"          # start_date du DAG ; le snapshot couvre [epoch, START)
VEILLE = (dt.date.fromisoformat(START) - dt.timedelta(days=1)).isoformat()
FIN = dt.datetime.now(dt.timezone.utc).date().isoformat()   # journées closes uniquement
TABLES = [("hotels", "hotel_id"), ("customers", "customer_id"),
          ("bookings", "booking_id"), ("payments", "payment_id")]

bq = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
raw, stg = os.environ["BQ_DATASET_RAW"], os.environ["BQ_DATASET_STAGING"]


def pg(sql):
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "booking",
         "-d", "booking_db", "-At", "-F", "|", "-c", sql],
        capture_output=True, text=True, check=True).stdout.split()


def q(sql):
    return list(bq.query(sql).result())


fenetre = (f"CASE WHEN updated_at < timestamptz '{START} 00:00+00' THEN date '{VEILLE}' "
           "ELSE (updated_at AT TIME ZONE 'UTC')::date END")
defauts = 0
for t, pk in TABLES:
    src = {d: int(n) for d, n in (l.split("|") for l in pg(
        f"SELECT {fenetre}, count(*) FROM {t} "
        f"WHERE updated_at < timestamptz '{FIN} 00:00+00' GROUP BY 1"))}
    dst = {str(r[0]): r[1] for r in q(f"SELECT _interval_start, COUNT(*) FROM `{raw}.{t}` GROUP BY 1")}
    ecarts = {d: (src.get(d, 0), dst.get(d, 0))
              for d in set(src) | set(dst) if src.get(d, 0) != dst.get(d, 0)}
    n, nd = q(f"SELECT COUNT(*), COUNT(DISTINCT {pk}) FROM `{raw}.{t}`")[0]
    s = {int(i): float(e) for i, e in (l.split("|") for l in pg(
        f"SELECT {pk}, extract(epoch FROM created_at) FROM {t}"))}
    dec = Counter(round(r.e - s[r.id], 3) if r.id in s else "absent" for r in
                  q(f"SELECT {pk} id, UNIX_MICROS(created_at)/1e6 e FROM `{raw}.{t}`"))
    ok = not ecarts and n == nd and set(dec) <= {0.0}
    defauts += not ok
    print(f"{'OK ' if ok else 'KO '} {t:<10} partitions={len(dst):>2} lignes={n} clés={nd} "
          f"décalages={dict(dec)} écarts={ecarts or 'aucun'}")

d_src = int(pg("SELECT count(*) - count(DISTINCT (booking_id, payment_method, paid_at)) "
               "FROM payments WHERE paid_at IS NOT NULL")[0])
d_stg = q(f"SELECT COUNTIF(is_duplicate_submission) FROM `{stg}.stg_payments`")[0][0]
b_src = int(pg("SELECT count(*) FROM bookings")[0])
b_stg = q(f"SELECT COUNT(*) FROM `{stg}.stg_bookings`")[0][0]
for nom, a, b in [("doubles soumissions", d_src, d_stg), ("réservations", b_src, b_stg)]:
    defauts += a != b
    print(f"{'OK ' if a == b else 'KO '} {nom} : source {a} / stg {b}")

print("\nRÉCONCILIATION CONFORME" if defauts == 0 else f"\n{defauts} ÉCART(S) À EXPLIQUER")
sys.exit(1 if defauts else 0)
