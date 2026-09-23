-- Grain : un client, dernière version connue.
-- Nettoyage : voir stg_customers_versions (source unique des règles).
select *
from {{ ref('stg_customers_versions') }}
qualify row_number() over (partition by customer_id order by updated_at desc) = 1
