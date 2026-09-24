-- Grain : une VERSION d'un client (SCD2). Clé : customer_sk.
-- Construite depuis l'historique de la raw, recréée à chaque run, sans DML :
-- le snapshot dbt exige un MERGE, refusé en bac à sable (ADR-034).
-- Bornes : valid_from NULL pour la première version (valide « depuis
-- toujours »), valid_to NULL pour la courante. Une première version bornée
-- par updated_at perdait 297 réservations sur 2 114 (23/09) ; bornée par
-- created_at, elle perdait les réservations antérieures à leur client
-- (dimension tardive, 4 cas le 24/09). customer_since et observed_from
-- gardent les dates réelles.
-- Limite : un rejeu de customers efface l'historique (87 clients le 22/09).
with v as (select * from {{ ref('stg_customers_versions') }})
select
    to_hex(md5(concat(cast(customer_id as string), '|', cast(updated_at as string)))) as customer_sk,
    customer_id, first_name, last_name, email, loyalty_tier,
    created_at as customer_since,
    if(row_number() over (partition by customer_id order by updated_at) = 1,
       null, updated_at) as valid_from,
    lead(updated_at) over (partition by customer_id order by updated_at) as valid_to,
    updated_at as observed_from,
    lead(updated_at) over (partition by customer_id order by updated_at) is null as is_current
from v
