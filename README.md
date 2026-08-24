# Booking Data Platform

> Plateforme de données répliquant une base transactionnelle PostgreSQL vers
> BigQuery, en batch puis en change data capture, avec modélisation
> dimensionnelle historisée et orchestration Airflow.

## Le problème métier

<!-- À RÉÉCRIRE AVEC TES MOTS — c'est la première chose que lit un recruteur.
     Trame : une chaîne hôtelière fictive encaisse des réservations dans une base
     transactionnelle. Les analystes ont besoin de répondre à des questions
     historiques ("quel était le niveau de fidélité du client AU MOMENT de la
     réservation ?") que la base de production ne sait pas restituer, parce
     qu'elle est écrasée en permanence. -->

## Le modèle source (OLTP)

| Table       | Grain                    | Rôle                                             |
|-------------|--------------------------|--------------------------------------------------|
| `hotels`    | un hôtel                 | ville, nombre d'étoiles                          |
| `customers` | un client                | email, `loyalty_tier` — **change dans le temps** |
| `bookings`  | une réservation          | dates, statut, montant                           |
| `payments`  | un paiement              | **pas de FK vers `bookings`** (cf. ADR-002)      |

## Architecture cible

```
                          ┌──────────────────────────┐
                          │   PostgreSQL 16 (OLTP)   │
                          │  hotels · customers      │
                          │  bookings · payments     │
                          │  wal_level = logical     │
                          └────┬────────────────┬────┘
                               │                │
        extraction incrémentale│                │ CDC — lecture du WAL
        (watermark updated_at) │                │ Debezium
                               ▼                ▼
                        ┌─────────────┐   ┌───────────┐
                        │  Parquet    │   │ Redpanda  │
                        │  data/raw/  │   │  (Kafka)  │
                        └──────┬──────┘   └─────┬─────┘
                               │                │ consumer Python
                               │                │ micro-batch
                               ▼                ▼
                     ┌──────────────────────────────────┐
                     │  BigQuery — raw_booking          │
                     └───────────────┬──────────────────┘
                                     │  dbt
                                     ▼
                     ┌──────────────────────────────────┐
                     │  staging_booking  →  marts_booking│
                     │  dim_* · fct_bookings · SCD2      │
                     └───────────────┬──────────────────┘
                                     ▼
                              Looker Studio

  Orchestration : Airflow (DAG quotidien, backfill idempotent)
  Infra as code : Terraform    CI : GitHub Actions
```

## Lancer le projet

<!-- À compléter au fil des sprints. -->

## Décisions d'architecture

Voir [DECISIONS.md](./DECISIONS.md).
