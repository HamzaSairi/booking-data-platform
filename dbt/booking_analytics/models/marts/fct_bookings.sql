{{ config(cluster_by=['booking_date', 'hotel_id']) }}
-- Pas de partitionnement (ADR-033) : en bac à sable, toute partition de plus
-- de 60 jours expire à sa création. Mesuré : 757 réservations sur 2 114
-- perdues avec un run vert. Le clustering élague les blocs sans expiration.
-- GRAIN : une ligne = une réservation, dans son dernier état connu.
-- Mesures additives : total_amount (réservé), amount_paid (encaissé, hors
-- doubles soumissions), nights.
-- Anomalies exposées, jamais filtrées : montant à zéro, surencaissement.
-- Hors grain : paiements orphelins, sans réservation (jour 20).
with b as (select * from {{ ref('stg_bookings') }}),
     p as (select * from {{ ref('int_payments_by_booking') }})
select
    b.booking_id,
    b.customer_id,
    b.hotel_id,
    date(b.created_at) as booking_date,
    b.check_in,
    b.check_out,
    date_diff(b.check_out, b.check_in, day) as nights,
    date_diff(b.check_in, date(b.created_at), day) as lead_time_days,
    b.status,
    b.currency,
    b.total_amount,
    coalesce(p.amount_paid, 0) as amount_paid,
    coalesce(p.payments_count, 0) as payments_count,
    coalesce(p.duplicate_submissions_count, 0) as duplicate_submissions_count,
    b.total_amount = 0 as is_zero_amount,
    coalesce(p.amount_paid, 0) > b.total_amount as is_overpaid,
    b.created_at,
    b.updated_at
from b
left join p using (booking_id)
