"""Simulateur de la base transactionnelle Booking.

Commandes :
    seed      — peuple la base avec un jeu de données initial cohérent
    simulate  — (Jour 4) fait vivre la base : modifications, défauts
"""
import os
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import click
import psycopg
from dotenv import load_dotenv
from faker import Faker
from psycopg import conninfo

load_dotenv()

# ─── Constantes ──────────────────────────────────────────────────────
CITIES = [
    ("Paris", "FR"), ("Lyon", "FR"), ("Marseille", "FR"), ("Nice", "FR"),
    ("Barcelona", "ES"), ("Madrid", "ES"), ("Sevilla", "ES"),
    ("Roma", "IT"), ("Milano", "IT"), ("Venezia", "IT"),
    ("Lisboa", "PT"), ("Porto", "PT"), ("Amsterdam", "NL"),
    ("Berlin", "DE"), ("Munchen", "DE"), ("Wien", "AT"),
    ("Bruxelles", "BE"), ("Geneve", "CH"), ("Praha", "CZ"), ("Athina", "GR"),
]

HOTEL_PREFIXES = ["Hotel", "Grand Hotel", "Residence", "Auberge", "Villa"]

# Prix moyen d'une nuit selon le nombre d'etoiles.
BASE_PRICE = {1: 45, 2: 70, 3: 110, 4: 180, 5: 320}

STATUS_FUTURE = (["confirmed", "pending", "cancelled"], [75, 15, 10])
STATUS_PAST = (["completed", "cancelled"], [92, 8])

TIER_WEIGHT = {"standard": 1, "silver": 2, "gold": 3}


# ─── Utilitaires ─────────────────────────────────────────────────────
def connect() -> psycopg.Connection:
    """Ouvre une connexion a partir des variables POSTGRES_* du .env."""
    try:
        info = conninfo.make_conninfo(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ["POSTGRES_DB"],
        )
    except KeyError as exc:
        raise SystemExit(f"Variable manquante dans .env : {exc.args[0]}") from exc
    return psycopg.connect(info)


def now_utc() -> datetime:
    return datetime.now(UTC)


def random_datetime_between(start: datetime, end: datetime) -> datetime:
    """Instant uniformement tire dans [start, end]."""
    span = (end - start).total_seconds()
    return start + timedelta(seconds=random.uniform(0, span))


def fetch_returned_ids(cur) -> list[int]:
    """Collecte les ids renvoyes par un executemany(..., returning=True).

    executemany produit UN jeu de resultats par ligne inseree, d'ou la
    boucle sur nextset() : sans elle, on ne recupere que le premier id.
    """
    ids = []
    while True:
        ids.append(cur.fetchone()[0])
        if not cur.nextset():
            break
    return ids


# ─── Insertions ──────────────────────────────────────────────────────
def insert_hotels(cur, faker: Faker, n: int) -> list[tuple[int, int]]:
    """Insere n hotels. Renvoie (hotel_id, stars) dans l'ordre d'insertion."""
    now = now_utc()
    start = now - timedelta(days=730)

    rows = []
    for _ in range(n):
        city, country = random.choice(CITIES)
        stars = random.choices([1, 2, 3, 4, 5], weights=[5, 15, 40, 30, 10])[0]
        name = f"{random.choice(HOTEL_PREFIXES)} {faker.last_name()}"
        created = random_datetime_between(start, now)
        rows.append((name, city, country, stars, created, created))

    cur.executemany(
        """
        INSERT INTO hotels (name, city, country, stars, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING hotel_id
        """,
        rows,
        returning=True,
    )
    ids = fetch_returned_ids(cur)
    return list(zip(ids, (r[3] for r in rows)))

def insert_customers(cur, faker: Faker, n: int) -> list[tuple[int, datetime, str]]:
    """Insere n clients. Renvoie (customer_id, created_at, loyalty_tier)."""
    now = now_utc()
    start = now - timedelta(days=548)          # 18 mois

    rows = []
    for i in range(n):
        first, last = faker.first_name(), faker.last_name()
        # Email derive du nom (et non faker.email()) : au Jour 19, on doit
        # pouvoir verifier a l'oeil qu'un email modifie appartient au bon client.
        email = f"{first}.{last}{i}@example.com".lower().replace(" ", "")
        tier = random.choices(
            ["standard", "silver", "gold"], weights=[70, 20, 10]
        )[0]
        created = random_datetime_between(start, now)
        rows.append((first, last, email, tier, created, created))

    cur.executemany(
        """
        INSERT INTO customers (first_name, last_name, email, loyalty_tier,
                               created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING customer_id
        """,
        rows,
        returning=True,
    )
    ids = fetch_returned_ids(cur)
    return [(cid, r[4], r[3]) for cid, r in zip(ids, rows)]


def insert_bookings(cur, hotels, customers, n: int, days: int) -> list[tuple]:
    """Insere n reservations. Renvoie (booking_id, status, amount, currency, created_at)."""
    now = now_utc()
    window_start = now - timedelta(days=days)
    today = now.date()

    # Correlation volontaire : un client gold reserve 3x plus qu'un standard.
    weights = [TIER_WEIGHT[tier] for _, _, tier in customers]

    rows = []
    for _ in range(n):
        cust_id, cust_created, _ = random.choices(customers, weights=weights)[0]
        hotel_id, stars = random.choice(hotels)

        # Une reservation ne peut pas preceder la creation de son client.
        created = random_datetime_between(max(window_start, cust_created), now)

        lead_days = min(int(random.expovariate(1 / 25)) + 1, 180)
        check_in = created.date() + timedelta(days=lead_days)
        nights = random.randint(1, 14)
        check_out = check_in + timedelta(days=nights)

        amount = (
            Decimal(nights)
            * Decimal(BASE_PRICE[stars])
            * Decimal(str(round(random.uniform(0.8, 1.4), 2)))
        ).quantize(Decimal("0.01"))

        # Le statut est DERIVE du temps, jamais tire au hasard : une
        # reservation 'pending' dont le sejour est termine n'existe pas.
        choices, w = STATUS_PAST if check_out < today else STATUS_FUTURE
        status = random.choices(choices, weights=w)[0]

        rows.append((cust_id, hotel_id, check_in, check_out, status,
                     amount, "EUR", created, created))

    cur.executemany(
        """
        INSERT INTO bookings (customer_id, hotel_id, check_in, check_out,
                              status, total_amount, currency,
                              created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING booking_id
        """,
        rows,
        returning=True,
    )
    ids = fetch_returned_ids(cur)
    return [(bid, r[4], r[5], r[6], r[7]) for bid, r in zip(ids, rows)]


def insert_payments(cur, bookings) -> int:
    """Un paiement capture par reservation confirmed ou completed."""
    rows = []
    for booking_id, status, amount, currency, created in bookings:
        if status not in ("confirmed", "completed"):
            continue          # on ne paie pas une reservation annulee
        paid_at = created + timedelta(hours=random.uniform(0, 48))
        rows.append((booking_id, amount, currency,
                     random.choice(["card", "transfer", "paypal", "cash"]),
                     "captured", paid_at, paid_at, paid_at))

    cur.executemany(
        """
        INSERT INTO payments (booking_id, amount, currency, payment_method,
                              status, paid_at, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    return len(rows)


# ─── CLI ─────────────────────────────────────────────────────────────
@click.group()
def cli() -> None:
    """Simulateur de la base Booking."""


@cli.command()
@click.option("--hotels", "n_hotels", default=50, show_default=True)
@click.option("--customers", "n_customers", default=500, show_default=True)
@click.option("--bookings", "n_bookings", default=2000, show_default=True)
@click.option("--days", default=90, show_default=True,
              help="Fenetre de repartition des reservations.")
@click.option("--seed", "rng_seed", default=42, show_default=True,
              help="Graine des generateurs aleatoires (cf. ADR-004).")
@click.option("--truncate", is_flag=True,
              help="Vide les tables avant insertion.")
def seed(n_hotels, n_customers, n_bookings, days, rng_seed, truncate) -> None:
    """Peuple la base avec un jeu de donnees initial."""
    random.seed(rng_seed)
    Faker.seed(rng_seed)
    faker = Faker("fr_FR")

    with connect() as conn, conn.cursor() as cur:
        if truncate:
            cur.execute(
                "TRUNCATE payments, bookings, customers, hotels "
                "RESTART IDENTITY CASCADE"
            )
        else:
            existing = cur.execute("SELECT count(*) FROM hotels").fetchone()[0]
            if existing:
                raise SystemExit(
                    f"{existing} hotels deja en base. "
                    "Relance avec --truncate pour repartir de zero."
                )

        hotels = insert_hotels(cur, faker, n_hotels)
        click.echo(f"{len(hotels):>6} hotels")

        # A DECOMMENTER au fur et a mesure :
        customers = insert_customers(cur, faker, n_customers)
        click.echo(f"{len(customers):>6} clients")
        bookings = insert_bookings(cur, hotels, customers, n_bookings, days)
        click.echo(f"{len(bookings):>6} reservations")
        n_pay = insert_payments(cur, bookings)
        click.echo(f"{n_pay:>6} paiements")


if __name__ == "__main__":
    cli()