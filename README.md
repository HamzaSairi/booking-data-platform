# Booking Data Platform

> Plateforme de données répliquant une base transactionnelle PostgreSQL vers
> BigQuery, en batch puis en change data capture, avec modélisation
> dimensionnelle historisée et orchestration Airflow.

## Le problème métier

Le directeur commercial d'une chaîne hôtelière demande un chiffre simple : le taux d'annulation par segment de clientèle. L'analyste le lui fournit, et il est faux.

Il est faux parce que la base transactionnelle est conçue pour l'écriture, pas pour la mémoire. Le statut de fidélité d'un client est écrasé à chaque promotion, si bien qu'une réservation faite en mars par un client « standard » apparaît aujourd'hui comme une réservation « gold ». Les réservations supprimées sortent purement des statistiques. Et une réservation confirmée puis annulée le même jour est comptée comme une simple annulation, ce qui masque exactement le cas qui intéresse le directeur : les clients qui se rétractent après s'être engagés.

Aucune alerte ne se déclenche, aucun pipeline n'échoue. Le chiffre est simplement plausible et faux. Ce projet s'attaque à cette cause.

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
   SOURCE OLTP              INGESTION            ENTREPOT           SERVICE

  +--------------+
  |  PostgreSQL  |  ==[batch]==>  extract  ==>  raw_booking
  |   16 (WAL    |                watermark      (partitionne)
  |   logical)   |                 J7-J9              |
  |              |                                    v
  |  4 tables    |                              staging_booking
  |  + triggers  |                                dbt / vues
  +--------------+                                    |
         ^                                            v
         |                                      marts_booking
  +--------------+                             faits + dimensions
  |  simulateur  |                              SCD2 (J19)
  |   Python     |                                    |
  |  seed /      |                                    v
  |  simulate    |                            .. Looker Studio (J29)
  +--------------+

  .. Debezium -> Redpanda -> consumer  (sprint 5, J21-J25)
  .. Airflow orchestre l'ensemble      (sprint 3, J11-J15)
  .. Terraform + GitHub Actions        (sprint 6, J26-J27)

## Lancer le projet

Prérequis : Docker, Python 3.11+, `make`.

```bash
cp .env.example .env    # renseigner POSTGRES_PASSWORD
make check              # démarre, peuple, teste
make simulate           # fait vivre la base
```

## Décisions d'architecture

Voir [DECISIONS.md](./DECISIONS.md).
