-- Les versions d'un client s'enchaînent sans trou ni chevauchement, avec
-- exactement une version courante et une seule version ouverte vers le passé.
-- Un trou ferait perdre des réservations, un chevauchement les dupliquerait.
with v as (
    select customer_id, valid_to,
           lead(valid_from) over (partition by customer_id order by observed_from) as next_from
    from {{ ref('dim_customers') }}
)
select customer_id, 'trou ou chevauchement' as probleme
from v
where next_from is not null and (valid_to is null or valid_to != next_from)
union all
select customer_id, 'versions courantes != 1'
from {{ ref('dim_customers') }}
group by customer_id
having countif(is_current) != 1
union all
select customer_id, 'premières versions != 1'
from {{ ref('dim_customers') }}
group by customer_id
having countif(valid_from is null) != 1
