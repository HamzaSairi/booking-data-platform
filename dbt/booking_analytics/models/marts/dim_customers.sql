-- Grain : un client, état actuel (SCD type 1).
-- Jour 19 : historisation SCD2 pour joindre la version valide à la date de réservation.
select customer_id, first_name, last_name, email, loyalty_tier,
       created_at as customer_since
from {{ ref('stg_customers') }}
