
## Infrastructure et CI (jours 26–27)

- **BigQuery sandbox** : expiration automatique des tables à 60 jours. Inacceptable en production : une perte de données source silencieuse. → Compte de facturation, budgets et alertes.
- **State Terraform local** : pas de travail en équipe, pas de verrouillage, pas de `plan` en CI. → Backend GCS versionné.
- **Clé de SA longue durée** dans un secret GitHub. → Workload Identity Federation (jetons temporaires, aucune clé).
- **Tests BigQuery hors CI** : la régression du test d'idempotence est restée invisible deux semaines. → Projet ou dataset de test dédié, tests d'intégration planifiés (nightly) plutôt qu'à chaque commit.
- **Aucun `terraform plan` sur les PR** : un changement d'infrastructure est relu uniquement en local.
