-- Grain : un hôtel, dernière version connue. Déduplication à la lecture (ADR-013).
select hotel_id, name, city, country, stars, created_at, updated_at
from {{ source('raw_booking', 'hotels') }}
qualify row_number() over (
    partition by hotel_id order by updated_at desc, _interval_start desc) = 1
