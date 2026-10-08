
## Infrastructure et CI (jours 26–27)

- **BigQuery sandbox** : expiration automatique des tables à 60 jours. Inacceptable en production : une perte de données source silencieuse. → Compte de facturation, budgets et alertes.
- **State Terraform local** : pas de travail en équipe, pas de verrouillage, pas de `plan` en CI. → Backend GCS versionné.
- **Clé de SA longue durée** dans un secret GitHub. → Workload Identity Federation (jetons temporaires, aucune clé).
- **Tests BigQuery hors CI** : la régression du test d'idempotence est restée invisible deux semaines. → Projet ou dataset de test dédié, tests d'intégration planifiés (nightly) plutôt qu'à chaque commit.
- **Aucun `terraform plan` sur les PR** : un changement d'infrastructure est relu uniquement en local.

## Observabilité (jour 28)

- `pipeline_metrics` relit un fichier JSONL unique à chaque run : acceptable
  à 60 Ko. En production : rotation quotidienne ou collecteur de logs.
- Partitions expirées à 60 jours (sandbox) : deux mois d'historique de durées.
- Scheduler arrêté = aucun callback. La requête « pipeline arrêté » le voit,
  mais seulement si quelqu'un la lance : en production, alerte externe.
- Les requêtes de détection ne sont pas planifiées : ce sont des vérifications
  manuelles, pas des alertes.

## Durée de vie des données (sandbox)

- Partitions et tables expirent à 60 jours. Échéance mesurée : **2026-10-30**,
  perte de 98 % des hôtels, 52 % des paiements, 42 % des réservations raw.
- L'historique SCD2 ne se reconstruit pas depuis le seed.
- En production : compte de facturation, aucune expiration sur raw et
  snapshots, expiration explicite uniquement sur les tables temporaires.

## Le SCD2 n'est durable que tant que la raw l'est (jour 29)
`dim_customers` (914 versions, 511 clients) est reconstruite par dbt depuis la
raw. Les partitions de `raw_booking.customers` expirent à partir du 30/10/2026
(bac à sable, 60 jours) : tout `dbt run` postérieur perd les versions les plus
anciennes, sans erreur. Copie locale : `data/backup/dim_customers_2026-10-07.parquet`.
En production : sortir du bac à sable, ou matérialiser l'historique en snapshot
dbt, qui conserve ses lignes même si la source les perd.
Biais connu : 49 % des réservations ont un niveau différent de l'actuel, bien
au-delà d'un programme réel, car le simulateur change les niveaux au hasard.

## Remboursements jamais exercés (jour 29)
Aucune réservation annulée n'a de paiement (379 annulées, 0 € encaissé) : le
simulateur annule avant paiement. Le cas « payé puis annulé » (remboursement,
statut `refunded`) n'est ni généré ni testé ; le modèle le compterait comme
encaissé sur une réservation annulée.
Le taux d'encaissement (94,7 %) est un solde net : 29 réservations surpayées
compensent une partie des impayés. Le dashboard affiche les deux séparément.
