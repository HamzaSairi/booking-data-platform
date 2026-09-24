{{ config(severity='warn') }}
-- Défaut injecté (late_arriving) : réservation antérieure à son client.
select booking_id, customer_id from {{ ref('fct_bookings') }} where is_late_arriving_customer
