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
     p as (select * from {{ ref('int_payments_by_booking') }}),
     c as (select customer_sk, customer_id, customer_since, valid_from, valid_to from {{ ref('dim_customers') }})
select
    b.booking_id,
    b.customer_id,
    c.customer_sk,   -- version du client valide au moment de la réservation
    b.created_at < c.customer_since as is_late_arriving_customer,   -- dimension tardive, exposée
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
left join c
  on c.customer_id = b.customer_id
 and (c.valid_from is null or b.created_at >= c.valid_from)
 and (b.created_at < c.valid_to or c.valid_to is null)
