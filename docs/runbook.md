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
6. **`tasks clear -t` filtre par sous-chaîne littérale** (observé au
   jour 15) : `^bookings\.` et `b.okings` ne correspondent à rien. Toujours
   inclure le point du groupe : `-t 'bookings.'`, pour ne pas attraper un
   futur groupe `bookings_cdc`.
7. **Un `clear` sans tâche correspondante rend la main sans rien
   afficher.** Pas de liste, pas de question : 0 tâche nettoyée.
8. **Tâche `upstream_failed` : les dates affichées sont celles de sa
   dernière exécution réelle**, pas du run en cours.
9. **Un état seul ne dit rien pendant les relances.** Deux relevés
   `running` successifs peuvent être deux tentatives différentes : c'est la
   `start_date` qui identifie la tentative.

## Outils

Toutes les commandes se lancent depuis la racine du dépôt.

```bash
alias af='docker compose exec airflow-scheduler airflow'

# Journal des incidents : une ligne JSON par relance ou échec de tâche
JOURNAL=airflow/logs/pipeline/evenements.jsonl
tail -n0 -F "$JOURNAL"     # -F attend la création du fichier, -n0 masque l'historique
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

Le champ `scenario_runbook` vaut `S1`, `S1b`, `S2a` ou `S2b` et renvoie aux
sections ci-dessous. **`null` signale un incident que ce runbook ne couvre pas
encore** : après résolution, ajouter le scénario et sa signature dans
`airflow/dags/commun/callbacks.py`.

Latence de détection, calculable depuis le journal seul : horodatage de la
première ligne `relance` moins sa `duree_tentative_s`, jusqu'à la ligne
`echec`.

## Incidents mesurés

| Date | Expérience | Scénario | Configuration | Détection | Reprise (échec → run vert) | Incident total |
|---|---|---|---|---|---|---|
| 10/09 | A — Postgres arrêté, redémarré après la 1re relance | S1 | plafond de relance 10 min | aucun échec : run vert après 10 min 11 s de relances | automatique | 10 min 11 s |
| 10/09 | B — Postgres arrêté | S1 | plafond 10 min | 30 min 12 s ¹ | 4 min 25 s | 34 min 37 s |
| 10/09 | C — Postgres gelé | S1b (classé `null` à l'époque) | `connect_timeout` par défaut (130 s), plafond 2 min | 14 min 43 s ² | 4 min 20 s | 19 min 03 s |
| 10/09 | C' — Postgres gelé | S1b | `connect_timeout` 10 s, plafond 2 min | 6 min 44 s ² | 3 min 05 s | 9 min 49 s |

¹ Depuis l'arrêt de Postgres (10:11:43 → 10:41:55).
² Depuis le démarrage de la première tentative d'`extract`, calculé depuis le
journal. Postgres était gelé avant : en production s'ajouterait l'attente du
prochain run planifié.

**Lecture.** La reprise reste entre 3 et 4 min 30 s d'un incident à l'autre,
alors que le rejeu lui-même prend environ 9 s : le reste est du temps humain.
La détection va de 6 min 44 s à 30 min 12 s selon la configuration. Formule vérifiée
sur C et C' :

```
détection ≈ (retries + 1) × durée d'une tentative en échec + retries × délai de relance
```

---

## S1 — Postgres injoignable (refus immédiat)

**Statut** : testé le 10/09/2026 (expériences A et B du jour 14).
**Mesuré** : détection 30 min 12 s (tâche déjà rejouée, plafond de relance
à 10 min, avant l'ADR-025), résolution 4 min 25 s, partition intacte pendant
la panne (632). **Non re-mesuré** avec le plafond de 2 min.

### Symptômes

- Les tâches `extract` passent en `up_for_retry`, puis `failed`.
  `validate` et `load` sont en `upstream_failed`, le run en `failed`.
- Journal : lignes `relance` puis `echec`, `exception_type` =
  `OperationalError`, `scenario_runbook` = `S1`, `duree_tentative_s` < 1.
- Un groupe `upstream_failed` ne produit **aucune** ligne : seules les
  tâches réellement exécutées déclenchent un callback.
- Chaque tentative échoue en moins d'une seconde : la détection vaut
  presque uniquement `retries × max_retry_delay`, soit ≈ 6 min avec le
  plafond actuel (prédit, non re-mesuré). Ce délai est atteint dès la
  première relance si la tâche a déjà été rejouée (try_number cumulé à
  travers les clear).

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
| `failed to resolve host 'postgres'` | conteneur arrêté, absent du réseau Docker (observé, expérience A) |
| `connection timeout expired` | serveur muet : ce n'est pas S1, voir **S1b** |
| `password authentication failed` | identifiants modifiés : ce n'est pas une panne, les relances n'y changeront rien (non observé) |

> Vérifier d'abord que c'est bien la **base source** qui est tombée et non la
> base de métadonnées d'Airflow : dans ce second cas, c'est le scheduler
> lui-même qui échoue, et aucun callback ne s'exécute.

### Résolution

```bash
docker compose start postgres               # gelé : voir S1b
docker compose exec postgres pg_isready -U booking -d booking_db
# attendre « accepting connections »

af dags list-runs ingestion_batch --state failed

# Rejouer failed + upstream_failed sur la période touchée.
# Sans --yes : vérifier la liste avant de répondre y.
af tasks clear ingestion_batch -s <AAAA-MM-JJ> -e <AAAA-MM-JJ> --only-failed
# Un seul groupe : ajouter -t '<groupe>.' (sous-chaîne, principe 6)
```

Panne de plusieurs jours : les intervalles jamais planifiés sont rattrapés
seuls (`catchup=True`), mais les runs déjà `failed` le restent. Élargir la
période du `clear` pour les couvrir.

### Vérification

- Runs concernés en `success`, avec une `end_date` **postérieure** à
  l'échec, et aucune nouvelle ligne `echec` au journal.
- Réconciliation source / cible de l'intervalle (requêtes de S3). Seule, une
  égalité ne prouve rien si la source n'a pas bougé depuis la panne : la
  preuve de la reprise est l'état `success` et sa `end_date`.

### Prévention en place

- **Plafond de relance à 2 min** (ADR-025) : borne la détection à ≈ 6 min.
- **`connect_timeout` = 10 s** (ADR-027) : sans effet ici, où l'échec est
  immédiat ; il sert S1b.
- **Pas d'`execution_timeout`** : reporté (ADR-027).

---

## S1b — Postgres muet (gelé, machine saturée, réseau qui perd les paquets)

**Statut** : testé le 10/09/2026 (expérience C du jour 15, deux fois :
avant et après l'ADR-027, via `docker compose pause`).
**Mesuré** : voir « Incidents mesurés », lignes C et C'.

### Différence avec S1

S1 : le serveur refuse ou n'existe plus, chaque tentative échoue en moins
d'une seconde. S1b : le serveur ne répond jamais, chaque tentative attend
`connect_timeout` avant d'échouer. Sans timeout explicite, psycopg 3.3.4
attend 130 s (constante privée `_DEFAULT_CONNECT_TIMEOUT`), ce qui a porté la
détection à 14 min 43 s.

### Symptômes

- Journal : `exception_type` = `ConnectionTimeout`, `scenario_runbook` =
  `S1b`, `duree_tentative_s` ≈ 10.
- `extract` est `running` pendant chaque tentative, puis `up_for_retry`.
  Seule la `start_date`, qui change d'une tentative à l'autre, révèle les
  relances (principe 9).
- Échec définitif ≈ 4 × 10 s + 3 × 2 min ≈ 6 min 44 s après le démarrage.
- **Non couvert** : un gel survenant *après* la connexion, en pleine
  requête. La tâche resterait `running` sans limite, sans callback
  (ADR-027).

### Impact

Identique à S1 : aucune écriture en cible, données retardées, pas perdues.

### Diagnostic

```bash
docker compose ps postgres        # « Paused » dans le statut : cause trouvée (local)
docker stats --no-stream          # sinon : CPU ou mémoire saturés ?
docker compose exec postgres pg_isready -U booking -d booking_db
```

Un serveur sain répond en moins d'une seconde.

### Résolution

Rétablir le serveur (`docker compose unpause postgres` en local), attendre
que `pg_isready` réponde `accepting connections`, puis appliquer la
résolution de S1 : `tasks clear --only-failed`, sans `--yes`.

**Ne pas rétablir le serveur pendant qu'une tentative est `running`** si
l'on mesure : sa connexion en attente aboutirait, le run passerait vert et
l'incident ne laisserait que des lignes `relance`.

### Vérification

Comme S1 : run en `success` avec une `end_date` postérieure à l'échec.

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
backfill concurrent (ADR-023), tempête de relances, déclenchements répétés.
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

- fenêtre nulle, `data_interval_start == data_interval_end` (ADR-022) ;
- amorçage concurrent détruisant des partitions (ADR-023) ;
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
reste dans le pipeline (ADR-023).

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
  Mesuré : la reprise prend de 3 à 4 min 30 s, dont environ 9 s de rejeu ; le reste est
  le temps qu'un humain remarque et décide. En production, une ligne
  `echec` déclencherait une notification.
- **Un blocage après la connexion ne produit rien.** Un blocage à la
  connexion devient un échec en 10 s (S1b), mais une requête gelée ou un
  chargement suspendu laisse la tâche `running` sans limite : pas
  d'`execution_timeout` (ADR-027). Le zombie du jour 13 n'aurait déclenché
  aucun callback non plus. Les SLA d'Airflow 2 ont été retirées en 3.0 ;
  leur remplaçant, les Deadline Alerts, n'est pas mis en place ici.
- **Les délais de relance dominent la détection** : 6 min sur 6 min 44 s
  en S1b. Le prochain levier est `retries` ou le plafond, pas le timeout.
- **Un callback en échec** est consigné dans les logs d'Airflow, jamais
  remonté.
- **S2 n'a jamais été exécuté.**
