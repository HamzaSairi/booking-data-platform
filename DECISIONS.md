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
