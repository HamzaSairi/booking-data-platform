{{ config(severity='warn') }}
-- Défaut injecté (zero_amount) : réservation à 0 €, autorisée par la source.
select booking_id from {{ ref('fct_bookings') }} where is_zero_amount
