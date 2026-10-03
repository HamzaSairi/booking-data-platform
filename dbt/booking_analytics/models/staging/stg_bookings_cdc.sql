-- Grain : une réservation, dernier événement CDC connu (ADR-038).
-- Mêmes colonnes et mêmes règles que stg_bookings (devise normalisée, montants
-- à zéro non filtrés) : fct_bookings change de source sans rien changer d'autre.
-- En plus : is_deleted, last_op, last_lsn.
-- Ordre : lsn, puis offset Kafka. Les doublons at-least-once (même offset)
-- portent le même contenu : le row_number en garde un, indifféremment.
-- Suppression : after est nul, before est complet (REPLICA IDENTITY FULL).
with evenements as (
    -- Pas coalesce(after, before) : un DELETE stocke after comme null JSON,
    -- que coalesce ne saute pas (seul le NULL SQL l'est). Vécu le 03/10.
    select op, lsn, _kafka_offset, if(op = 'd', before, after) as ligne
    from {{ source('raw_booking_cdc', 'bookings_cdc') }}
),
dernier as (
    select *
    from evenements
    qualify row_number() over (
        partition by int64(ligne.booking_id)
        order by lsn desc, _kafka_offset desc) = 1
)
select
    int64(ligne.booking_id) as booking_id,
    int64(ligne.customer_id) as customer_id,
    int64(ligne.hotel_id) as hotel_id,
    date_from_unix_date(int64(ligne.check_in)) as check_in,
    date_from_unix_date(int64(ligne.check_out)) as check_out,
    string(ligne.status) as status,
    cast(string(ligne.total_amount) as numeric) as total_amount,
    upper(trim(string(ligne.currency))) as currency,
    timestamp(string(ligne.created_at)) as created_at,
    timestamp(string(ligne.updated_at)) as updated_at,
    op = 'd' as is_deleted,
    op as last_op,
    lsn as last_lsn
from dernier
