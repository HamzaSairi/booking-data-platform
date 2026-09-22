-- Grain : une réservation, dernière version connue.
-- Défaut normalisé ici : devise en casse variable (ADR-005).
-- Montants à zéro NON filtrés : règle métier, testée au jour 20.
select
    booking_id, customer_id, hotel_id, check_in, check_out, status,
    total_amount,
    upper(trim(currency)) as currency,
    created_at, updated_at
from {{ source('raw_booking', 'bookings') }}
qualify row_number() over (
    partition by booking_id order by updated_at desc, _interval_start desc) = 1
