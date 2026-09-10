# Runbook — `ingestion_batch`

Procédures de résolution des incidents connus du DAG `ingestion_batch`
(Postgres → Parquet → BigQuery `raw_booking`). Chaque scénario suit le même
ordre : **symptômes → impact → diagnostic → résolution → vérification**.

Chaque scénario porte un statut. Une procédure jamais exécutée est une
hypothèse, et elle est étiquetée comme telle.

## Principes

1. **Rejouer est toujours sûr.** Extraction par fenêtre, chemin Parquet
   déterministe, écrasement de partition : un intervalle rejoué N fois donne
   le même résultat qu'une fois (jours 9 et 13). En cas de doute, on rejoue.
2. **Toute reprise passe par Airflow** (`tasks clear`, `backfill create`),
   jamais par un script lancé à la main. L'ordonnanceur est la seule mémoire
   de ce qui a été traité.
3. **Un changement d'état fait à la main ne déclenche aucun callback.**
   Marquer une tâche `failed` dans l'UI ou la CLI ne teste pas la
   surveillance ; seul un échec réel à l'exécution le fait.
4. **Un run vert ne prouve pas que la cible est juste.** Voir S3.
5. **Lancer `tasks clear` sans `--yes` d'abord** : la commande liste ce
   qu'elle va nettoyer et demande confirmation.

## Outils

```bash
alias af='docker compose exec airflow-scheduler airflow'

# Journal des incidents : une ligne JSON par relance ou échec de tâche
JOURNAL=<chemin hôte>/logs/pipeline/evenements.jsonl
tail -f "$JOURNAL"
grep -c '"evenement": "echec"' "$JOURNAL"

# Vue compacte
python -c "
import json, sys
for l in open(sys.argv[1]):
    e = json.loads(l)
    print(e['horodatage'][11:19], e['evenement'][:7], e['tentative'],
          e['task_id'], e['scenario_runbook'], (e['exception_message'] or '')[:60])
" "$JOURNAL"
```

Le champ `scenario_runbook` vaut `S1`, `S2a` ou `S2b` et renvoie aux sections
ci-dessous. **`null` signale un incident que ce runbook ne couvre pas
encore** : après résolution, ajouter le scénario et sa signature dans
`airflow/dags/commun/callbacks.py`.

---

## S1 — Postgres injoignable

**Statut** : testé le ____ (expériences A, B, C du jour 14).
**Durée de résolution mesurée** : ____ min.

### Symptômes

- Les tâches `extract` passent en `up_for_retry`, puis `failed`.
  `validate` et `load` sont en `upstream_failed`, le run en `failed`.
- Journal : lignes `relance` puis `echec`, `exception_type` =
  `OperationalError`, `scenario_runbook` = `S1`.
- Un groupe `upstream_failed` ne produit **aucune** ligne : seules les
  tâches réellement exécutées déclenchent un callback.

### Impact

Aucune écriture en cible : `load` n'a pas tourné, la partition BigQuery de
l'intervalle est intacte. Les données sont **retardées, pas perdues** : la
source les conserve et la fenêtre sera rejouée.

### Diagnostic

```bash
docker compose ps postgres                  # arrêté ? redémarre en boucle ?
docker compose logs --tail 50 postgres      # OOM, disque plein, crash
docker compose exec postgres pg_isready -U booking -d booking_db
docker system df                            # disque de la VM Docker
```

Lire `exception_message` dans le journal :

| Le message contient | Cause probable |
|---|---|
| <!-- message réel, expérience B --> | conteneur arrêté, absent du réseau Docker |
| <!-- message réel, expérience C --> | Postgres gelé ou saturé |
| `password authentication failed` | identifiants modifiés : ce n'est pas une panne, les relances n'y changeront rien |

> Vérifier d'abord que c'est bien la **base source** qui est tombée et non la
> base de métadonnées d'Airflow : dans ce second cas, c'est le scheduler
> lui-même qui échoue, et aucun callback ne s'exécute.

### Résolution

```bash
docker compose start postgres               # ou `unpause` si gelé
docker compose exec postgres pg_isready -U booking -d booking_db
# attendre « accepting connections »

af dags list-runs ingestion_batch --state failed

# Rejouer failed + upstream_failed sur la période touchée
af tasks clear ingestion_batch -s <AAAA-MM-JJ> -e <AAAA-MM-JJ> --only-failed
```

Panne de plusieurs jours : les intervalles jamais planifiés sont rattrapés
seuls (`catchup=True`), mais les runs déjà `failed` le restent. Élargir la
période du `clear` pour les couvrir.

### Vérification

- Runs concernés en `success`, aucune nouvelle ligne `echec` au journal.
- Réconciliation source / cible de l'intervalle (requêtes de S3) : égalité
  attendue, puisque le rejeu vient de lire l'état actuel de la source.

### Prévention en place

- `connect_timeout` sur la connexion psycopg : une base gelée devient un
  échec franc, relancé et journalisé, au lieu d'une tâche bloquée.
- `execution_timeout` sur toutes les tâches : filet pour tout autre blocage.

---

## S2 — BigQuery refuse le chargement

**Statut** : **non testé en conditions réelles.** Procédure fondée sur la
documentation BigQuery (troubleshoot-quotas), à confirmer au premier incident.

### Symptômes

- `extract` et `validate` verts, `load` en échec.
- Journal : `exception_type` = `Forbidden` (403) ou `TooManyRequests` (429).

### Deux incidents sous le même code HTTP

| | S2a — limite de débit | S2b — quota |
|---|---|---|
| Message | `Exceeded rate limits` | `Quota exceeded` |
| `reason` | `rateLimitExceeded` | `quotaExceeded` |
| Horizon | quelques secondes | reconstitution sur 24 h glissantes |
| Les relances aident ? | oui | **non** : attendre au moins 10 min, et chaque job échoué est décompté du quota |

### Impact

Un load job est atomique : en échec, il n'écrit rien. La partition cible est
intacte et les Parquet validés sont toujours sur disque.

### Diagnostic

À lancer avec **ton compte personnel** : le service account du pipeline n'a
pas `bigquery.jobs.listAll`, par moindre privilège.

```sql
-- Volume de chargements par table sur 24 h
SELECT destination_table.table_id AS table_cible,
       COUNT(*) AS load_jobs_24h,
       COUNTIF(error_result IS NOT NULL) AS en_echec
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND job_type = 'LOAD'
GROUP BY 1 ORDER BY 2 DESC;

-- Dernières erreurs, avec le quota exact nommé dans le message
SELECT creation_time, destination_table.table_id,
       error_result.reason, error_result.message
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND job_type = 'LOAD' AND error_result IS NOT NULL
ORDER BY creation_time DESC LIMIT 20;
```

Au rythme nominal (quatre chargements par jour), le projet est très loin des
plafonds. Un quota atteint signifie donc presque toujours **une boucle** :
backfill concurrent (ADR-016), tempête de relances, déclenchements répétés.
Le diagnostic cherche la boucle, pas le quota.

### Résolution

**S2a** — rien si le run finit vert. Si l'erreur revient pendant un
backfill : relancer celui-ci avec `--max-active-runs 1`.

**S2b** :

```bash
af dags pause ingestion_batch      # 1. plus aucun nouveau job (les tâches en cours finissent)
# 2. identifier et corriger la boucle avec les requêtes ci-dessus
# 3. attendre la reconstitution du quota
af dags unpause ingestion_batch
af tasks clear ingestion_batch -s <début> -e <fin> --only-failed
```

### À ne pas confondre

- L'alerte budget (jour 6) est une notification : elle ne bloque rien.
- Dépasser le niveau gratuit déclenche une facturation, pas une erreur.

### Amélioration identifiée, non implémentée

Dans la tâche `load` du DAG, intercepter `quotaExceeded` et lever
`AirflowFailException` : les trois relances actuelles, espacées de quelques
minutes, ne peuvent pas réussir et consomment du quota.

---

## S3 — Intervalle vert, cible fausse

**Statut** : causes observées réellement le jour 13 ; résolution démontrée
(deux backfills complets, 2 762 lignes, 0 doublon).

### Pourquoi ce scénario remplace « watermark corrompu »

Le plan prévoyait un watermark corrompu. Depuis le jour 13, il n'y a plus de
watermark : l'état vit dans la base de métadonnées d'Airflow et l'extraction
est une fonction pure de sa fenêtre. L'incident équivalent — l'état dit
« fait », la cible dit autre chose — existe toujours, sous sa forme la plus
dangereuse : **aucune tâche n'échoue, aucun callback ne se déclenche, le
journal reste vide.**

### Symptômes

Aucun côté Airflow. Découvert par réconciliation, par un test dbt (jour 20)
ou par un analyste. Causes rencontrées au jour 13 :

- fenêtre nulle, `data_interval_start == data_interval_end` (ADR-015) ;
- amorçage concurrent détruisant des partitions (ADR-016) ;
- run rejouant une version du DAG antérieure au correctif ;
- fonction de chargement sans `return` : aucun job soumis, tâche verte.

### Diagnostic : réconciliation par intervalle

```bash
# Source
docker compose exec postgres psql -U booking -d booking_db -c "
SELECT (updated_at AT TIME ZONE 'UTC')::date AS jour, count(*) AS source
FROM bookings
WHERE updated_at >= '<début>' AND updated_at < '<fin exclue>'
GROUP BY 1 ORDER BY 1;"

# Cible
bq query --use_legacy_sql=false \
"SELECT _interval_start AS jour, COUNT(*) AS cible
 FROM \`$GCP_PROJECT_ID.raw_booking.bookings\`
 WHERE _interval_start BETWEEN '<début>' AND '<dernier jour inclus>'
 GROUP BY 1 ORDER BY 1"
```

| Constat | Lecture |
|---|---|
| partition absente, source > 0 | **anomalie** : intervalle jamais chargé, ou détruit |
| cible < source | **anomalie** : chargement partiel ou partition écrasée |
| cible > source | dérive normale : des lignes ont été modifiées ou supprimées en source après l'extraction et ont quitté cette fenêtre (`docs/limites-batch.md`) |
| cible = source | sain |

Exceptions à la lecture : l'intervalle en cours (incomplet par nature), et
tout défaut du simulateur écrivant un `updated_at` dans le passé (ADR-003).

### Résolution

```bash
af backfill list                   # aucun backfill fantôme ne doit traîner (jour 13)
af dags list-runs ingestion_batch --state running

af backfill create --dag-id ingestion_batch \
  --from-date <début> --to-date <fin> \
  --reprocess-behavior completed --max-active-runs 1
```

`--max-active-runs 1` est **obligatoire** tant que la création des tables
reste dans le pipeline (ADR-016).

### Vérification

Refaire la réconciliation **immédiatement** après le backfill : égalité
stricte attendue sur chaque intervalle rejoué.

### Prévention

Aucune automatique à ce jour. Prévue : tests dbt de volume (jour 20),
fraîcheur des sources (jour 28). D'ici là : réconciliation manuelle après
tout backfill.

---

## Lacunes connues

- **Personne n'est prévenu.** Le journal est local : il faut aller le lire.
  En production, une ligne `echec` déclencherait une notification.
- **Un run bloqué ne produit rien.** Le zombie du jour 13 n'aurait déclenché
  aucun callback. Les SLA d'Airflow 2 ont été retirées en 3.0 ; leur
  remplaçant, les Deadline Alerts, n'est pas mis en place ici.
- **Un callback en échec** est consigné dans les logs d'Airflow, jamais
  remonté.
- **S2 n'a jamais été exécuté.**
