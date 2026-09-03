-- Journal d'audit des changements sur bookings.
-- Sert UNIQUEMENT de vérité terrain pour mesurer ce que le batch rate
-- (jour 10). Ce n'est pas du CDC : c'est intrusif, ça alourdit chaque
-- écriture, et ça ne couvre qu'une table. Le WAL contient déjà cette
-- information sans aucun de ces défauts — c'est l'argument du sprint 5.

CREATE TABLE IF NOT EXISTS bookings_audit (
    audit_id    bigserial PRIMARY KEY,
    booking_id  integer     NOT NULL,     -- adapte si booking_id est un uuid
    operation   char(1)     NOT NULL,     -- I / U / D
    old_status  text,
    new_status  text,
    changed_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_booking ON bookings_audit (booking_id);

CREATE OR REPLACE FUNCTION audit_bookings() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO bookings_audit (booking_id, operation, old_status, new_status)
        VALUES (NEW.booking_id, 'I', NULL, NEW.status);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO bookings_audit (booking_id, operation, old_status, new_status)
        VALUES (NEW.booking_id, 'U', OLD.status, NEW.status);
        RETURN NEW;
    ELSE
        INSERT INTO bookings_audit (booking_id, operation, old_status, new_status)
        VALUES (OLD.booking_id, 'D', OLD.status, NULL);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_bookings ON bookings;
CREATE TRIGGER trg_audit_bookings
AFTER INSERT OR UPDATE OR DELETE ON bookings
FOR EACH ROW EXECUTE FUNCTION audit_bookings();