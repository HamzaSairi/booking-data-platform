# Limites connues

Ce que ce projet ne fait pas, ou fait d'une manière qui ne passerait pas en
production. Chaque point vient d'un constat fait pendant le projet ; la
référence entre parenthèses renvoie à l'ADR ou au document qui le détaille.

## 1. Ce qui ne passerait pas en production

### Durabilité des données

- **Bac à sable BigQuery** (ADR-008, ADR-045, ADR-041). Tables et partitions
  expirent à 60 jours. Échéance mesurée : 2026-10-30, avec la perte de 98 % des
  hôtels, 52 % des paiements et 42 % des réservations de la couche raw. La perte
  est silencieuse. → Compte de facturation, budgets et alertes ; aucune
  expiration sur la raw et les snapshots, expiration explicite sur les seules
  tables temporaires.
- **Le SCD2 n'est durable que tant que la raw l'est** (ADR-034).
  `dim_customers` (914 versions, 511 clients) est reconstruite par dbt depuis la
  raw : tout `dbt run` postérieur à l'expiration perd les versions les plus
  anciennes, sans erreur. Copie locale :
  `data/backup/dim_customers_2026-10-07.parquet`. → Sortir du bac à sable, ou
  matérialiser l'historique en snapshot dbt, qui conserve ses lignes même si la
  source les perd.

### Sécurité et infrastructure

- **State Terraform local** (ADR-043) : ni travail en équipe, ni verrouillage.
  → Backend GCS versionné.
- **Clé de service account longue durée** dans un secret GitHub (ADR-047).
  → Workload Identity Federation : jetons temporaires, aucune clé stockée.
- **Aucun `terraform plan` sur les PR** (ADR-046) : un changement
  d'infrastructure n'est relu qu'en local.
- **Le compte de service peut créer des datasets** (ADR-035) : privilège à
  resserrer.
- **Le dashboard s'appuie sur mes identifiants personnels** (ADR-050).
  → Compte de service dédié, en lecture seule sur `marts`.

### Observabilité et CI

- **CI resté rouge deux jours sans être remarqué** (jour 30). Des fichiers du
  jour 28 ont été commités sans passer `ruff` sur tout le dépôt : pre-commit ne
  contrôle que les fichiers indexés. → Protection de branche (fusion
  impossible si le CI échoue) et notification d'échec.
- **Tests BigQuery exclus du CI** (ADR-048) : une régression du test
  d'idempotence est restée invisible deux semaines. → Projet ou dataset de test
  dédié, tests d'intégration planifiés chaque nuit.
- **Scheduler arrêté = aucun callback.** La requête « pipeline arrêté » le
  détecte, mais seulement si quelqu'un la lance. → Alerte externe au système
  surveillé.
- **Les requêtes de détection ne sont pas planifiées** : ce sont des
  vérifications manuelles, pas des alertes.
- **`pipeline_metrics` relit un fichier JSONL unique** à chaque run :
  acceptable à 60 Ko. → Rotation quotidienne ou collecteur de logs. Ses
  partitions expirent elles aussi à 60 jours : deux mois d'historique de durées.

- **La fraîcheur métier confond source calme et pipeline arrêté** (jour 30).
  Le test regarde la dernière fenêtre contenant des changements : après cinq
  jours sans activité en source, il était en erreur alors que chaque run
  quotidien avait réussi. → Mesurer séparément la fraîcheur du pipeline (dernier
  run réussi, `pipeline_metrics`) et celle des données.

### Couverture fonctionnelle

- **Remboursements jamais exercés** (jour 29). Aucune réservation annulée n'a
  de paiement (379 annulées, 0 € encaissé) : le simulateur annule avant tout
  paiement. Le cas « payé puis annulé » n'est ni généré ni testé ; le modèle le
  compterait comme encaissé sur une réservation annulée.
- **Le taux d'encaissement (94,7 %) est un solde net** : 29 réservations
  surpayées compensent une partie des impayés. Le dashboard affiche les deux
  séparément, mais l'indicateur seul trompe.
- **Ce que le CDC ne résout pas** (`docs/batch-vs-cdc.md`) : il ne reconstitue
  pas le passé antérieur au slot ; Kafka n'ordonne qu'au sein d'une partition,
  pas entre tables ; des faits CDC frais joints à des dimensions batch périmées
  ont produit 9 réservations sans client avant rattrapage.
- **Slot de réplication** : un slot que personne ne consomme retient le WAL.
  Il est borné à 1 Go (ADR-037) ; au-delà, Postgres invalide le slot, et il
  faut un nouvel instantané complet.

## 2. Biais du jeu de données

Les chiffres du projet sont réels, mais mesurés sur des données simulées.
Certains biais en gonflent l'effet :

- **49 % des réservations ont un niveau de fidélité différent du niveau
  actuel du client**, bien au-delà d'un programme réel : le simulateur change
  les niveaux au hasard. L'erreur de la jointure naïve (+136 % de CA attribué
  à gold, −69 % pour standard) démontre le **mécanisme**, pas son **ampleur**
  en conditions réelles.
- **Suppressions rares** : 14 sur 2 322 réservations lors de la mesure du
  jour 10. Les fantômes du batch sont un ordre de grandeur ; le chiffre robuste
  est celui des transitions perdues (21 sur 209).
- **Volume modeste** (~3 200 réservations) : aucun comportement à l'échelle n'a
  été mesuré (coût des requêtes, durée des réécritures de partition, lag du CDC
  sous charge).

## 3. Ce que je ferais avec plus de temps

Par ordre de priorité :

1. **Sortir du bac à sable et passer le SCD2 en snapshot dbt** : ce sont les
   deux seules limites qui détruisent des données.
2. **Achever la bascule vers le CDC**, dimensions comprises, puis retirer
   l'ingestion batch : l'architecture hybride crée des désynchronisations
   entre faits et dimensions.
3. **Planifier les requêtes de santé** et les relier à une notification.
4. **Protéger la branche `main`** derrière le CI.
5. **Ajouter des labels aux jobs BigQuery lancés en Python** (reporté du
   jour 28), pour attribuer les coûts par étape.
6. **Tester une évolution de schéma en source** de bout en bout (colonne
   ajoutée, renommée) : Debezium, consommateur, dbt.
7. **Générer un jeu 100 fois plus gros** pour mesurer coûts et lag.
