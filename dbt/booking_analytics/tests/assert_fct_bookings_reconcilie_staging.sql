-- Chaque réservation de staging est dans le fait, et inversement.
-- Compare des identifiants, pas des volumes : attrape une perte silencieuse
-- (expiration de partition, jointure qui élimine des lignes).
select 'absente du fait' as ecart, s.booking_id
from {{ ref('stg_bookings') }} s
left join {{ ref('fct_bookings') }} f using (booking_id)
where f.booking_id is null
union all
select 'absente de staging', f.booking_id
from {{ ref('fct_bookings') }} f
left join {{ ref('stg_bookings') }} s using (booking_id)
where s.booking_id is null
