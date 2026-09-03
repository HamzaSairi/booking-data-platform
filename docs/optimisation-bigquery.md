# Optimisation BigQuery — mesures

Fiche de mesures, pas un journal. Chaque entrée doit être reproductible
à partir du protocole ci-dessous, et chaque chiffre doit être daté.

La facturation BigQuery porte sur les **octets scannés**, jamais sur la
durée d'exécution. Une requête lente et une requête coûteuse sont deux
problèmes distincts, qui ne se résolvent pas avec les mêmes leviers.

## Protocole

    bq query --location=EU --use_legacy_sql=false --dry_run "<requête>"

Le `--dry_run` valide la requête et retourne le volume qui serait scanné,
sans exécuter ni facturer. La sortie annonce une **borne supérieure** :
BigQuery estime à partir des métadonnées sans ouvrir les fichiers. Les
chiffres ci-dessous sont reportés tels quels, sans arrondi.

Équivalent dans la console : coller la requête sans l'exécuter et lire
l'estimation affichée en haut à droite de l'éditeur.

## Table de référence

`raw_booking.bookings` au 2026-09-02 :

- 6 072 lignes, issues de 12 fichiers Parquet
- partitionnée par jour sur `_ingested_at`, une seule partition (`20260902`)
- clusterisée sur `booking_id`
- inclut les métadonnées techniques `_ingested_at` et `_source_file`

Le facteur de redondance de la table est de ~3 (relectures complètes
provoquées par des remises à zéro du watermark au jour 7). Il n'affecte
pas les rapports mesurés ici, qui comparent deux requêtes sur la même
table.

## Mesure 1 — Stockage colonnaire (2026-09-02)

| Requête | Octets scannés (borne sup.) |
|---|---|
| `SELECT * FROM bookings` | 995 090 |
| `SELECT booking_id FROM bookings` | 48 576 |
| **Rapport** | **20,5× — 95,1 % de scan en moins** |

**Lecture** : la seule différence entre les deux requêtes est la liste des
colonnes. Aucune clause `WHERE`, aucun élagage de partition (tout est dans
`20260902`). Le facteur 20,5 mesure donc uniquement l'effet du colonnaire :
`booking_id` représente environ un vingtième de la largeur de la ligne, et
BigQuery ne lit physiquement que cette fraction.

**Conséquence pratique** : `SELECT *` est le premier réflexe à perdre en
OLAP. En transactionnel il est quasi gratuit — Postgres lit la page entière
de toute façon. Ici il multiplie la facture par la largeur de la table.

## Mesure 2 — Élagage de partition

**Non mesurable au 2026-09-02.** Toutes les lignes appartiennent à une
partition unique : un filtre sur `_ingested_at` n'élimine rien, et toute
comparaison avant/après serait un artefact.

Prérequis pour mesurer honnêtement : au moins 5 à 7 jours de chargements
distincts, donc autant de partitions. À reprendre après le sprint 3, une
fois le DAG Airflow en exécution quotidienne.

Requête prévue :

```sql
SELECT booking_id, status
FROM `PROJET.raw_booking.bookings`
WHERE _ingested_at >= TIMESTAMP('<date>')
```

à comparer à la même requête sans clause `WHERE`.

## À faire au jour 28

Trois requêtes analytiques réalistes sur `marts_booking`, mesurées avant
et après ajout du partitionnement (`booking_date`) et du clustering
(`hotel_id`), sur un volume suffisant pour que l'élagage soit visible.

Objectif : une phrase chiffrée précise et défendable, du type « le
partitionnement sur la date de réservation réduit de X % les octets
scannés sur les trois requêtes du dashboard ». Pas de pourcentage sans
la requête et le volume qui vont avec.

## Journal des mesures

| Date | Mesure | Résultat |
|---|---|---|
| 2026-09-02 | Colonnaire sur `raw_booking.bookings` | 20,5× |