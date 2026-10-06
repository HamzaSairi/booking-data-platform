
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
