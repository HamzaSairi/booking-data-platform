# tests/conftest.py
"""Fixtures partagées par toute la suite de tests."""

import sys
from pathlib import Path

import pytest

# Rend `simulator` et `ingestion` importables depuis les tests.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from simulator.generate import connect  # noqa: E402


@pytest.fixture
def conn():
    """Connexion à la base de test, refermée après chaque test."""
    with connect() as c:
        yield c


@pytest.fixture
def cur(conn):
    """Curseur prêt à l'emploi — la plupart des tests n'ont besoin que de ça."""
    with conn.cursor() as c:
        yield c