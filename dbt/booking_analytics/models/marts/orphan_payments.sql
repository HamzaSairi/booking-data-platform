-- Grain : un paiement dont la réservation est introuvable.
-- Défaut connu (ADR-002 : pas de clé étrangère ; ADR-005 : injecté).
-- Hors du grain de fct_bookings : exposé ici, jamais masqué (ADR-036).
select p.*
from {{ ref('stg_payments') }} p
left join {{ ref('stg_bookings') }} b using (booking_id)
where b.booking_id is null
