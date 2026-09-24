{{ config(severity='warn') }}
-- Défaut injecté (duplicate_email) : deux clients, un même email (casse ignorée).
select email, count(*) as clients
from {{ ref('stg_customers') }} group by email having count(*) > 1
