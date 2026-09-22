-- Grain : un client, dernière version connue.
-- Email normalisé en minuscules : les tests d'unicité (jour 20) doivent
-- ignorer la casse. Le doublon injecté copie l'email à l'identique.
select
    customer_id, first_name, last_name,
    lower(trim(email)) as email,
    loyalty_tier, created_at, updated_at
from {{ source('raw_booking', 'customers') }}
qualify row_number() over (
    partition by customer_id order by updated_at desc, _interval_start desc) = 1
