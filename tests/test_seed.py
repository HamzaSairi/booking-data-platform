"""Verifie que le seed produit une base coherente, pas seulement peuplee."""
import pytest

from simulator.generate import connect


@pytest.fixture(scope="module")
def cur():
    with connect() as conn, conn.cursor() as c:
        yield c


def test_tables_peuplees(cur):
    for table, mini in [("hotels", 50), ("customers", 500), ("bookings", 2000)]:
        n = cur.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        assert n >= mini, f"{table} : {n} lignes"



def test_statut_coherent_avec_les_dates(cur):
    """Un sejour termine ne peut pas etre 'pending' ou 'confirmed'."""
    n = cur.execute("""
        SELECT count(*) FROM bookings
        WHERE check_out < CURRENT_DATE AND status IN ('pending', 'confirmed')
    """).fetchone()[0]
    assert n == 0


def test_donnees_etalees_dans_le_temps(cur):
    """Le piege du DEFAULT clock_timestamp() : tout au meme instant."""
    n = cur.execute(
        "SELECT count(DISTINCT created_at::date) FROM bookings"
    ).fetchone()[0]
    assert n >= 60


def test_correlation_fidelite(cur):
    """Un client gold doit reserver nettement plus qu'un standard."""
    rows = dict(cur.execute("""
        SELECT c.loyalty_tier,
               count(b.booking_id)::float / count(DISTINCT c.customer_id)
        FROM customers c LEFT JOIN bookings b USING (customer_id)
        GROUP BY 1
    """).fetchall())
    assert rows["gold"] > rows["silver"] > rows["standard"]

def test_reservations_anterieures_a_leur_client_restent_marginales(cur):
    """Defaut n°7 de l'ADR-003 : dimension a arrivee tardive, injectee.

    L'invariant reste vrai pour le chemin nominal ; ces anomalies sont
    ATTENDUES et mesurees plutot qu'interdites. Le seuil detecterait une
    derive : 2 % signalerait un bug du simulateur, pas une injection.
    """
    n, total = cur.execute("""
        SELECT count(*) FILTER (WHERE b.created_at < c.created_at), count(*)
        FROM bookings b JOIN customers c USING (customer_id)
    """).fetchone()
    assert n / total < 0.02, f"{n}/{total} reservations anterieures a leur client"

# tests/test_extract.py
def test_seconde_extraction_ne_produit_presque_rien():
    """At-least-once : un chevauchement borné est acceptable, une perte non."""