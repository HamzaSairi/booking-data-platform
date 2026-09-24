-- Une seule ligne par version (customer_id, updated_at) : sinon dim_customers
-- produirait deux versions de même début et dupliquerait des réservations.
select customer_id, updated_at, count(*) as n
from {{ ref('stg_customers_versions') }}
group by 1, 2
having count(*) > 1
