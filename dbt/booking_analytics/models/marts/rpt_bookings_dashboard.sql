{{ config(materialized='view') }}

-- Source unique du dashboard US-29 (docs/dashboard.md).
-- Grain : une ligne = une réservation, hérité de fct_bookings.
-- Aucune donnée personnelle exposée (minimisation RGPD).

with f as (select * from {{ ref('fct_bookings') }}),
     c as (select * from {{ ref('dim_customers') }}),
     h as (select * from {{ ref('dim_hotels') }}),
     d as (select * from {{ ref('dim_dates') }})

select
    f.booking_id,
    f.booking_date,
    date_trunc(f.booking_date, isoweek)            as booking_week,
    d.year_month,
    f.check_in,
    f.nights,
    f.lead_time_days,
    case
        when f.lead_time_days < 0   then '0. incohérent'
        when f.lead_time_days <= 7  then '1. 0-7 j'
        when f.lead_time_days <= 30 then '2. 8-30 j'
        when f.lead_time_days <= 90 then '3. 31-90 j'
        else '4. > 90 j'
    end                                            as lead_time_bucket,
    f.status,
    h.hotel_name, h.city, h.country, h.stars,

    -- Q4 : la démonstration SCD2
    c_hist.loyalty_tier                            as tier_at_booking,
    c_cur.loyalty_tier                             as tier_current,
    c_hist.loyalty_tier != c_cur.loyalty_tier      as tier_changed,
    f.is_late_arriving_customer,

    f.total_amount,
    f.amount_paid,
    f.payments_count,
    f.payments_count = 0                           as is_unpaid,
    f.duplicate_submissions_count,
    f.is_zero_amount,
    f.is_overpaid
from f
left join c as c_hist on c_hist.customer_sk = f.customer_sk
left join c as c_cur  on c_cur.customer_id  = f.customer_id and c_cur.is_current
left join h           on h.hotel_id         = f.hotel_id
left join d           on d.date_day         = f.booking_date
