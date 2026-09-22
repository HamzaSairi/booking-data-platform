-- Grain : un hôtel, état actuel.
select hotel_id, name as hotel_name, city, country, stars
from {{ ref('stg_hotels') }}
