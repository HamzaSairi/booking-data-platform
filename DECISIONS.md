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
