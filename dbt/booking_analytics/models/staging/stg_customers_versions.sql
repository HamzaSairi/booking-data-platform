-- Grain : une version d'un client (customer_id, updated_at), telle que vue
-- par l'ingestion : une version par chargement, changements intra-journaliers
-- perdus (ADR-006). Seul endroit où les clients sont nettoyés.
-- Une même version chargée deux fois (rejeu) : on garde la partition la plus récente.
-- Email normalisé en minuscules : les tests d'unicité (jour 20) doivent
-- ignorer la casse. Le doublon injecté copie l'email à l'identique.
select
    customer_id, first_name, last_name,
    lower(trim(email)) as email,
    loyalty_tier, created_at, updated_at
from {{ source('raw_booking', 'customers') }}
qualify row_number() over (
    partition by customer_id, updated_at order by _interval_start desc) = 1
