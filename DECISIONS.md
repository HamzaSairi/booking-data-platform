# Décisions d'architecture (ADR)

Format : contexte → options → décision → raison (avec son coût) → date.

---

## ADR-001 — Docker Compose plutôt qu'une installation locale

**Contexte** : le projet a besoin d'une base Postgres, puis plus tard d'Airflow,
de Redpanda et de Kafka Connect, avec une configuration Postgres non standard
(`wal_level=logical`).

**Options** : (a) installation native de chaque service, (b) Docker Compose,
(c) une VM dédiée.

**Décision** : Docker Compose.

**Raison** : reproductibilité (le même fichier donne le même environnement sur
n'importe quelle machine), remise à zéro instantanée via `docker compose down -v`
— ce qui rend testable le scénario "je repars de zéro" —, et le fichier YAML
documente l'architecture au même endroit que le code.
**Coût assumé** : une couche d'abstraction supplémentaire à déboguer (réseau,
volumes, permissions de fichiers), et une consommation mémoire notable une fois
Airflow et Redpanda ajoutés.

**Date** : 2026-08-24

## ADR-002 — Pas de clé étrangère entre payments et bookings

**Contexte** : dans une architecture réaliste, l'encaissement est géré par un
service distinct de la réservation, souvent avec sa propre base. La contrainte
d'intégrité référentielle n'existe alors pas au niveau du SGBD.

**Options** :
(a) `booking_id BIGINT REFERENCES bookings(booking_id)` — intégrité garantie
(b) `booking_id BIGINT NOT NULL` sans contrainte — couplage faible simulé
(c) FK avec `ON DELETE SET NULL`

**Décision** : (b).

**Raison** : le projet doit produire des défauts de qualité réalistes, pas une
base parfaite. L'option (a) rend structurellement impossible l'existence d'un
paiement orphelin, donc supprime le problème au lieu de savoir le détecter.
Avec (b), le simulateur pourra en injecter, et le Jour 20 les comptera dans une
table dédiée plutôt que de les masquer.

**Coût assumé** : la source ne garantit plus l'intégrité référentielle. C'est
au pipeline de la mesurer, et à moi de documenter le taux d'orphelins constaté.
Un jointure naïve dans les marts perdrait ces lignes silencieusement.

**Date** : 2026-08-25

## ADR-003 — REPLICA IDENTITY FULL sur les quatre tables

**Contexte** : en réplication logique, Postgres écrit toujours la nouvelle
version d'une ligne dans le WAL, mais l'ancienne dépend de la REPLICA IDENTITY
de la table. En mode DEFAULT, seule la clé primaire est journalisée.

**Options** :
(a) DEFAULT — `before` réduit à la PK sur les DELETE, `null` sur les UPDATE
(b) FULL — ligne complète avant modification
(c) USING INDEX sur un index unique restreint — compromis

**Décision** : (b) FULL.

**Raison** : deux démonstrations du projet en dépendent. Au Jour 23, observer un
`op='d'` avec son `before` renseigné. Au Jour 19, montrer la transition d'un
client de `standard` à `gold`. En DEFAULT, je saurais qu'un changement a eu lieu
sans savoir lequel — la moitié de la valeur du CDC disparaît.

**Coût assumé** : chaque UPDATE écrit l'ancienne ligne entière dans le WAL, donc
le volume de journal augmente. Négligeable à mon échelle (2000 réservations).
Sur une table de plusieurs millions de lignes fortement mise à jour, ce choix
serait discutable : on préférerait (c), ou on reconstituerait le `before` côté
entrepôt à partir de la version précédente.

**À vérifier** : mesurer le volume de WAL généré au Jour 22, une fois le
connecteur Debezium en place.

**Date** : 2026-08-25

## ADR-004 — Graine aléatoire fixe par défaut dans le simulateur

**Contexte** : le simulateur génère hôtels, clients et réservations par tirage
aléatoire. Deux exécutions produisent par défaut deux bases différentes.

**Options** :
(a) aléatoire pur, non reproductible
(b) graine fixe en dur
(c) graine fixe par défaut, surchargeable par --seed

**Décision** : (c), valeur par défaut 42.

**Raison** : le test de reproductibilité du Jour 5 (down -v puis reseed) n'a de
sens que si la base reconstruite est identique. Les mesures d'octets scannés du
Jour 28, avant et après partitionnement, ne sont comparables que sur le même
jeu de données. Et les captures d'écran du README resteraient cohérentes.

**Coût assumé** : un jeu figé ne révèle jamais les bugs qui n'apparaissent que
sur certaines valeurs — un nom contenant une apostrophe, un montant à zéro.
D'où l'option --seed conservée : lancer périodiquement avec une autre graine
est un test à part entière.

**Date** : 2026-08-26

## ADR-005 — Défauts de qualité injectés par le simulateur

**Contexte** : le projet doit démontrer une capacité à détecter et traiter des
problèmes de qualité. Une base parfaite ne prouve rien. Il faut donc des
défauts, mais choisis — pas du bruit aléatoire.

**Options** :
(a) aucun défaut, base propre
(b) défauts aléatoires non documentés
(c) un défaut par dimension de la qualité, documenté et journalisé
(d) (c) + retrait des CHECK du schéma pour couvrir toutes les dimensions

**Décision** : (c).

**Raison** : chaque défaut injecté doit correspondre à un test précis du
Jour 20 ; l'inverse — écrire des tests puis chercher quoi tester — produit des
tests décoratifs. L'inventaire des contraintes du schéma a servi de filtre :

| Défaut | Dimension | Détecté au |
|---|---|---|
| Paiement orphelin | intégrité référentielle | J20, table dédiée |
| Encaissement > montant réservé | cohérence | J20, test singulier |
| Doublon de paiement | unicité | J17, dédup ROW_NUMBER |
| Email dupliqué | unicité | J20, test `unique` |
| Devise non normalisée | validité | J17, normalisation staging |
| Montant à zéro | exactitude | J20, règle métier (> 0) |
| Réservation + client tardifs | complétude / fraîcheur | J13, backfill |

Deux constats issus de cet inventaire. Il n'existe aucun index unique sur
`customers.email` : les tests d'unicité ne peuvent donc pas se reposer sur le
schéma. Et `CHECK (total_amount >= 0)` autorise la valeur 0, qui n'a aucun sens
métier — la règle technique et la règle métier ne coïncident pas, la seconde
doit vivre dans les tests dbt.

L'option (d) est écartée : un CHECK en source est une garantie sur laquelle le
pipeline peut s'appuyer. Le retirer pour se donner du travail serait un
contresens. Quatre défauts sont donc structurellement impossibles ici
(montant négatif, dates inversées, statut inconnu, réservation orpheline) —
les tests correspondants seront quand même écrits, pour détecter le jour où
quelqu'un supprimerait la contrainte.

**Conséquence sur les tests** : `test_aucune_reservation_avant_son_client` est
devenu un test à seuil (< 2 %) plutôt qu'un absolu. Une donnée de production
n'est jamais parfaite ; ce qu'on surveille est le taux et sa dérive.

**Coût assumé** : les défauts sont injectés par un tirage indépendant par type
et par tour, ce qui ne reproduit pas les corrélations du réel — un incident de
production produit des défauts en rafale, pas un par un. Le fichier
`state/simulation_log.jsonl` conserve la vérité terrain, faute de quoi le
chiffrage du Jour 10 serait impossible : la source ne garde que l'état final
d'une ligne.

**Date** : 2026-08-27

## ADR-006 — Région EU pour les datasets BigQuery
**Contexte** : trois datasets à créer (raw, staging, marts) qui seront joints
entre eux par dbt à partir du jour 17.
**Options** : (a) US multi-région (défaut de `bq`), (b) EU multi-région,
(c) une région unique type europe-west9.
**Décision** : (b) EU.
**Raison** : les données simulent des réservations de clients européens, donc
le RGPD est l'argument métier. La vraie contrainte est technique : BigQuery
refuse de joindre deux datasets situés dans des régions différentes, et un
dataset ne se déplace pas après création. Oublier `--location=EU` sur un seul
des trois aurait cassé le premier `ref()` dbt avec un message peu explicite.
Le multi-région plutôt qu'une région unique pour la disponibilité, à coût
identique à ce volume.
**Coût** : décision irréversible. Toute source externe future devra être en EU.
**Date** : 2026-09-01

## ADR-007 — Clé de service account plutôt que credentials applicatives par défaut
**Contexte** : authentifier les scripts d'ingestion auprès de BigQuery.
**Options** : (a) `gcloud auth application-default login`, (b) clé JSON de
service account, (c) Workload Identity Federation.
**Décision** : (b).
**Raison** : Google déconseille les clés de service account en général, et à
juste titre — c'est un secret de longue durée sans expiration. Mais au jour 11
Airflow tournera dans un conteneur qui n'a pas accès à `~/.config/gcloud`, et
au jour 27 la CI n'a pas de session interactive. Un fichier montable est le
seul mécanisme commun aux trois environnements. (c) serait le bon choix en
production mais suppose une infrastructure d'identité que ce projet n'a pas.
**Coût** : secret de longue durée, stocké hors du dépôt dans ~/.gcp (chmod 600),
rotation manuelle. À reconsidérer si le projet passait sur une VM GCP, où le
service account attaché rendrait la clé inutile.
**Date** : 2026-09-01

## ADR-008 — Bac à sable BigQuery, faute de compte de facturation
**Contexte** : le plan prévoit une alerte budget à 5 €. Les trois comptes de
facturation disponibles sur ce compte Google sont clôturés (`OPEN=False`) et
l'essai gratuit n'est pas réattribuable.
**Options** : (a) créer un compte de facturation avec carte bancaire,
(b) utiliser le bac à sable BigQuery, (c) changer de fournisseur cloud.
**Décision** : (b), avec (a) en recours si une contrainte bloque.
**Raison** : le volume du projet (quelques centaines de Mo, chargements batch
gratuits) tient largement dans le niveau gratuit. Le bac à sable donne 10 Go de
stockage et 1 To de requêtes par mois sans carte.
**Conséquences acceptées** :
- expiration automatique des tables à 60 jours — le projet dure six semaines,
  mais les captures d'écran de la vitrine devront être prises avant la fin ;
- aucune alerte budget possible ;
- pas de streaming inserts. Sans impact : le jour 24 prévoit déjà des
  micro-batchs par load jobs. Ne pas dériver vers `insert_rows_json`.
**Parade sur les coûts** : `maximum_bytes_billed` à 10 Gio dans
`ingestion/bq.py`, appliqué à toute requête. Une alerte budget notifie après
coup ; le plafond fait échouer le job avant exécution. C'est le garde-fou le
plus fort des deux.
**À revoir** : jour 26, si Terraform exige un compte de facturation actif.
**Date** : 2026-09-01

## ADR-009 — Extraction incrémentale : `>` avec marge de sécurité
**Contexte** : l'extraction doit ne lire que les lignes modifiées depuis le
dernier passage, via `WHERE updated_at > watermark`.

**Le problème réel** — plus large que le classique `>` vs `>=` :
1. `>=` relit systématiquement la dernière ligne : doublon garanti, jamais de
   perte.
2. `>` l'exclut. Mais `updated_at` n'est pas unique : si N lignes partagent le
   même timestamp et que l'extraction s'interrompt au milieu, les suivantes
   sont perdues silencieusement.
3. Pire, et indépendant du choix de l'opérateur : Postgres date une ligne au
   DÉBUT de sa transaction, pas au commit. Une transaction ouverte à 10:00:00
   et commitée à 10:00:05 écrit une ligne datée 10:00:00. Si le pipeline passe
   à 10:00:03, cette ligne apparaît APRÈS son passage avec un timestamp
   ANTÉRIEUR : invisible pour toujours.

**Décision** : `>`, plus une marge de sécurité de 5 secondes (`SAFETY_MARGIN`)
qui recule la borne, plus une déduplication en aval au Jour 17
(`ROW_NUMBER() OVER (PARTITION BY pk ORDER BY updated_at DESC)`).

**Raison** : aucune valeur de watermark ne règle le point 3 — c'est une limite
structurelle de l'extraction par requête, et c'est l'argument qui justifiera
Debezium au sprint 5. On choisit donc le compromis : accepter un doublon
détectable plutôt qu'une perte silencieuse.

**Coût mesuré** : 28 lignes relues sur 1720 paiements à la seconde exécution,
soit 1,6 % de redondance, et 1 ligne pour chacune des trois autres tables.
Zéro perte.

**Date** : 2026-09-01

## ADR-010 — Ordre écrire-puis-avancer, et écriture atomique de l'état
**Contexte** : l'extraction produit un fichier Parquet et met à jour un
watermark persisté dans `state/watermarks.json`.
**Options** : (a) avancer le watermark puis écrire, (b) écrire puis avancer,
(c) transaction distribuée entre le disque et le fichier d'état.
**Décision** : (b), avec `sauver_etat` qui écrit dans un `.tmp` puis fait un
`replace()` atomique.
**Raison** : un plantage entre les deux étapes est inévitable à terme (disque
plein, processus tué, coupure). Dans l'ordre (b) il produit un doublon, que la
déduplication absorbe ; dans l'ordre (a) il produit une perte définitive.
(c) est hors de portée et rarement justifié.
C'est la garantie **at-least-once**, la même qu'au Jour 24 avec le commit
d'offset Kafka : on ne confirme jamais avant d'avoir écrit.
Le `replace()` protège du symétrique : un JSON tronqué rendrait l'état
illisible au tour suivant.
**Coût** : doublons à traiter en aval. Assumé.
**Date** : 2026-09-01

## ADR-011 — Garde d'identité de base dans le fichier d'état
**Contexte** : `state/watermarks.json` vit sur le disque hôte et survit à un
`docker compose down -v`, qui détruit pourtant le volume Postgres.
**Décision** : stocker `pg_control_system().system_identifier` à côté des
watermarks et refuser de démarrer s'il diffère de celui de la base connectée.
**Raison** : sans cette garde, une base recréée avec 2000 réservations neuves
et un watermark resté à hier produit une extraction à 0 ligne, sans erreur.
Testé réellement : le message explicite s'est affiché comme prévu.
Le problème avait déjà été rencontré au Jour 5 avec `simulation_log.jsonl`.
**Coût** : deux minutes de code, une requête supplémentaire au démarrage.
**Date** : 2026-09-01