# Préparation d'entretien

Réponses orales (~45 s) appuyées sur ce que le projet a réellement fait, avec
les preuves à ouvrir dans le dépôt.

## 0. Le pitch (30 s)

J'ai construit une plateforme qui réplique une base PostgreSQL de réservations
hôtelières vers BigQuery. J'ai d'abord fait du batch incrémental, puis j'ai
**mesuré** ce qu'il ratait : 21 transitions de statut perdues sur 209, et des
réservations supprimées qui restaient dans l'entrepôt. C'est cette mesure qui
m'a fait passer au CDC avec Debezium et Redpanda, qui ramène l'écart à zéro.
Côté modélisation, j'ai historisé le niveau de fidélité des clients en SCD2 :
sans ça, le CA attribué aux clients gold est surévalué de 136 %. Le tout est
orchestré par Airflow, testé (94 tests), décrit en Terraform, et chaque
décision est tracée dans 50 ADR.

## 1. Comment retraites-tu 3 mois de données sans dupliquer ?

Deux conditions. D'abord, chaque run traite **sa** fenêtre, dérivée de
l'intervalle de données d'Airflow, jamais de `now()` : sinon un backfill
retraite toujours aujourd'hui. J'ai d'ailleurs révisé ma première approche sur
ce point (ADR-022 → ADR-028). Ensuite, l'écriture **remplace** la partition de
la fenêtre au lieu d'ajouter des lignes : rejouer un jour dix fois donne le même
résultat (ADR-020). J'ai gardé `max_active_runs=1` pour ménager la source et
les quotas ; la contrepartie, constatée, c'est qu'un run en échec bloque les
suivants (ADR-021).
Limite honnête : dans mon projet, 3 mois seraient impossibles. La raw est en
bac à sable et expire à 60 jours.

**Preuves** : `airflow/dags/ingestion/ingestion_dag.py`,
`tests/test_idempotence.py`, ADR-020, ADR-028, ADR-029 (amorçage initial).

## 2. CDC par log ou par requête : quelle différence, et quand choisir chacun ?

Par requête, on interroge `updated_at` : on ne voit que l'état final au moment
du passage. J'ai mesuré ce que ça coûte : 21 transitions perdues sur 209, et
des suppressions invisibles, parce qu'un `DELETE` ne modifie aucun
`updated_at`. J'ai eu jusqu'à 17 réservations fantômes en cible.
Par log, Debezium lit le WAL : chaque changement, suppressions comprises,
avec l'état avant et après. Réconciliation : 0 écart sur 3 205 identifiants.
Le prix : deux conteneurs de plus, un slot de réplication à surveiller, un lag.
Je choisirais la requête pour du reporting quotidien sans suppressions, ou
quand la base n'expose pas sa réplication logique. Le log, dès que les
suppressions ou l'historique des états comptent.

**Preuves** : `docs/limites-batch.md`, `docs/batch-vs-cdc.md`,
`tests/test_reconciliation_cdc.py`, ADR-014.

## 3. Comment détectes-tu qu'un pipeline a échoué silencieusement ?

En testant le **résultat**, pas l'exécution. Mon pipeline batch était vert
alors qu'il gardait des réservations supprimées : c'est l'échec silencieux
type. J'ai donc ajouté un test de fraîcheur par source, un test de volume
plancher (le nombre de réservations ne peut pas baisser, puisque les
suppressions étaient invisibles), une réconciliation source/cible, et une
table `pipeline_metrics` alimentée par les callbacks Airflow, avec des
requêtes de santé.
Ce que j'ai appris à mes dépens : mon CI est resté rouge deux jours sans que
je le voie, et un scheduler arrêté ne déclenche aucun callback. Une détection
qu'il faut lancer à la main n'est pas une alerte.

**Preuves** : `dbt/booking_analytics/tests/assert_volume_plancher.sql`,
`airflow/dags/observabilite/`, `docs/runbook.md`, ADR-040.

## 4. Explique le SCD Type 2 et pourquoi c'est important.

Un attribut qui change, comme le niveau de fidélité, n'est pas écrasé : chaque
changement crée une nouvelle version, avec une date de début et une date de
fin. La réservation est jointe à la version **valide à la date de la
réservation**.
Sur mon dashboard, la jointure naïve sur la version actuelle attribue à gold
136 % de CA en trop et retire 69 % à standard, à total identique. Aucun test
ne casse, c'est juste faux. Je précise que l'ampleur est gonflée par mon
simulateur, qui change les niveaux au hasard : 49 % des réservations sont
concernées, bien plus qu'en réalité.
J'ai reconstruit l'historique depuis la raw plutôt qu'avec un snapshot dbt
(ADR-034). La contrepartie : il n'est durable que tant que la raw l'est.

**Preuves** : `dim_customers`, `docs/dashboard.md`, `docs/img/dashboard1.png`,
ADR-034, ADR-050.

## 5. Comment optimises-tu une requête BigQuery coûteuse ?

D'abord je mesure, parce que BigQuery facture les octets lus, pas le temps.
Le premier levier est de ne lire que les colonnes utiles : 20,5 fois moins
d'octets sur ma table. Ensuite, partitionnement et clustering. Le
partitionnement réduisait les octets lus de 77 % sur une fenêtre de 7 jours…
et le gain facturé était de 0 %, parce que BigQuery facture au minimum 10 Mo
par requête. En bac à sable, il aurait en plus fait expirer 32 % des lignes.
J'ai donc gardé le clustering seul. J'ai aussi constaté qu'un dry-run ne
voit pas l'élagage du clustering : il faut mesurer en exécution réelle.

**Preuves** : `docs/optimisation-bigquery.md`, `scripts/mesure_octets.py`,
ADR-033.

## 6. Comment testes-tu de la donnée, pas seulement du code ?

Avec des invariants : unicité et non-nullité des clés, relations, valeurs
acceptées, `check_out > check_in`, montants positifs, paiement ≤ montant
réservé, volume plancher, fraîcheur. Ma politique de sévérité : les
invariants en `error`, les défauts **connus** de la source en `warn` et
comptés, la dérive des taux en `error` (ADR-036). Les paiements orphelins ne
sont pas filtrés en silence : ils ont leur propre table.
La leçon la plus utile est venue du dashboard : un contrôle croisé Python /
Looker Studio concordait au centime, alors que mon « CA confirmé » oubliait le
statut `completed`, soit 1,66 M€. Les deux appliquaient le même filtre faux.
Un contrôle croisé vérifie le calcul, pas la définition.

**Preuves** : `dbt/booking_analytics/tests/`, les `.yml` des modèles, ADR-036,
`JOURNAL.md` (jour 29).

## 7. Que se passe-t-il si ton consommateur Kafka plante en plein micro-batch ?

L'offset n'est commité qu'**après** l'écriture réussie dans BigQuery. Si le
consommateur plante avant, il relit les messages au redémarrage : aucune
perte, mais des doublons. Je l'ai provoqué : 0 perte, 1 000 doublons. Ils sont
absorbés dans dbt, qui garde le dernier événement par clé, ordonné par LSN.
C'est le compromis at-least-once plus déduplication en aval (ADR-038) :
l'exactly-once de bout en bout vers BigQuery coûterait bien plus cher à
construire qu'un `ROW_NUMBER()`.

**Preuves** : `ingestion/` (consommateur), `stg_bookings_cdc`, ADR-038,
`docs/batch-vs-cdc.md`.

## 8. Comment gères-tu un changement de schéma en source ?

Je ne l'ai **pas testé** dans ce projet, et je le dis. Ce que je sais du
chemin : Debezium publie le nouveau schéma avec les événements, mon
consommateur charge dans BigQuery, et dbt lit la raw. Une colonne ajoutée
peut passer si le chargement autorise l'ajout de champs. Une colonne
renommée ou retypée cassera le staging, ce qui est souhaitable : mieux vaut
un échec visible qu'une colonne vide en silence.
Pour le traiter sérieusement, je ferais l'essai de bout en bout, j'ajouterais
un test qui compare les colonnes attendues aux colonnes réelles de la raw, et
une règle claire : ajout toléré, suppression et renommage bloquants.

**Preuves** : `docs/limites.md` (partie 3, point 6).

## 9. Pourquoi Debezium plutôt que Datastream ?

Datastream est le CDC managé de Google : il écrit directement dans BigQuery,
sans Kafka à exploiter. En production sur GCP, avec une petite équipe, c'est
souvent le bon choix.
Ici, deux raisons. Une contrainte : je suis en bac à sable, sans compte de
facturation, et Datastream est un service payant. Et un objectif : comprendre
la mécanique que Datastream cache, c'est-à-dire le slot de réplication, le
WAL qui grossit si personne ne consomme (je l'ai borné à 1 Go), les offsets,
le lag, l'ordre par LSN. Ce sont précisément les questions qu'on se pose le
jour où un service managé se comporte mal.

**Preuves** : `debezium/connector-postgres.json`, ADR-037, ADR-045.

## 10. Qu'est-ce qui, dans ton projet, ne passerait pas en production ?

Trois choses, par gravité. Un : le bac à sable. Les tables expirent à 60
jours, le 30/10 je perds 42 % des réservations de la raw, et mon historique
SCD2 avec, puisqu'il en est reconstruit. Il me faut un compte de facturation,
ou un snapshot dbt qui conserve l'historique. Deux : la sécurité. Une clé de
compte de service longue durée dans un secret GitHub, au lieu de Workload
Identity Federation, et un state Terraform local. Trois : les alertes. Mes
requêtes de santé existent, mais personne n'est prévenu sans les lancer. Mon
CI est resté rouge deux jours sans que je m'en aperçoive.
J'ajoute que mes chiffres viennent de données simulées : le mécanisme du SCD2
est démontré, pas son ampleur réelle.

**Preuves** : `docs/limites.md`.
