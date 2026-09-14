"""Fixtures partagees par les tests qui parlent a la base source."""

import os

import psycopg
import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="session")
def cur():
    """Curseur sur la base source, partage par tous les fichiers de tests."""
    with psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        connect_timeout=10,
    ) as conn, conn.cursor() as c:
        yield c
