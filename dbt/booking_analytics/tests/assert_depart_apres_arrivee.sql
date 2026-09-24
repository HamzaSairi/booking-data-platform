-- Invariant (CHECK chk_dates en source) : une nuit au moins.
select booking_id, check_in, check_out
from {{ ref('fct_bookings') }}
where check_out <= check_in
