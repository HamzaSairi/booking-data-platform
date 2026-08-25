-- Schéma OLTP de la plateforme de réservation hôtelière.
-- Exécuté automatiquement au PREMIER démarrage du conteneur uniquement.

BEGIN;

-- ---------------------------------------------------------------
-- Fonction partagée : maintient updated_at à chaque modification.
-- ---------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------
-- hotels — dimension quasi statique
-- ---------------------------------------------------------------
CREATE TABLE hotels (
    hotel_id    BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL,
    city        TEXT        NOT NULL,
    country     CHAR(2)     NOT NULL,
    stars       SMALLINT    NOT NULL CHECK (stars BETWEEN 1 AND 5),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- ---------------------------------------------------------------
-- customers — dimension à changement lent (matière première du SCD2)
-- ---------------------------------------------------------------
CREATE TABLE customers (
    customer_id   BIGSERIAL PRIMARY KEY,
    first_name    TEXT        NOT NULL,
    last_name     TEXT        NOT NULL,
    email         TEXT        NOT NULL,
    loyalty_tier  TEXT        NOT NULL DEFAULT 'standard'
                  CHECK (loyalty_tier IN ('standard', 'silver', 'gold')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- ---------------------------------------------------------------
-- bookings — table de faits transactionnelle
-- ---------------------------------------------------------------
CREATE TABLE bookings (
    booking_id    BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT      NOT NULL REFERENCES customers(customer_id),
    hotel_id      BIGINT      NOT NULL REFERENCES hotels(hotel_id),
    check_in      DATE        NOT NULL,
    check_out     DATE        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'confirmed', 'cancelled', 'completed')),
    total_amount  NUMERIC(10,2) NOT NULL CHECK (total_amount >= 0),
    currency      CHAR(3)     NOT NULL DEFAULT 'EUR',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_dates CHECK (check_out > check_in)
);

-- ---------------------------------------------------------------
-- payments — service découplé : PAS de clé étrangère (voir ADR-002)
-- ---------------------------------------------------------------
CREATE TABLE payments (
    payment_id      BIGSERIAL PRIMARY KEY,
    booking_id      BIGINT      NOT NULL,          -- volontairement sans REFERENCES
    amount          NUMERIC(10,2) NOT NULL CHECK (amount >= 0),
    currency        CHAR(3)     NOT NULL DEFAULT 'EUR',
    payment_method  TEXT        NOT NULL
                    CHECK (payment_method IN ('card', 'transfer', 'paypal', 'cash')),
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'captured', 'refunded', 'failed')),
    paid_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- ---------------------------------------------------------------
-- Triggers
-- ---------------------------------------------------------------
CREATE TRIGGER trg_hotels_updated_at    BEFORE UPDATE ON hotels
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_bookings_updated_at  BEFORE UPDATE ON bookings
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_payments_updated_at  BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------
-- Index
-- ---------------------------------------------------------------
-- Extraction incrémentale (Jour 7) : un index par table sur updated_at
CREATE INDEX idx_hotels_updated_at    ON hotels    (updated_at);
CREATE INDEX idx_customers_updated_at ON customers (updated_at);
CREATE INDEX idx_bookings_updated_at  ON bookings  (updated_at);
CREATE INDEX idx_payments_updated_at  ON payments  (updated_at);

-- Accès applicatifs courants
CREATE INDEX idx_bookings_customer_id ON bookings (customer_id);
CREATE INDEX idx_bookings_hotel_id    ON bookings (hotel_id);
CREATE INDEX idx_bookings_status      ON bookings (status);
CREATE INDEX idx_payments_booking_id  ON payments (booking_id);

COMMIT;