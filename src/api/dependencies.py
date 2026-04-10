"""FastAPI dependency injection for database connections."""

import sqlite3
from typing import Generator

from src.core.config import DB_PATH
from src.core.db import connect


_conn: sqlite3.Connection | None = None


def get_db() -> Generator[sqlite3.Connection, None, None]:
    """Yield a shared read-optimised SQLite connection.

    SQLite in WAL mode supports concurrent readers, so a single long-lived
    connection is fine for the read-heavy API. Writes (scraping) happen in
    separate processes/threads with their own connections.
    """
    global _conn
    if _conn is None:
        _conn = connect()
        _conn.row_factory = sqlite3.Row
    yield _conn


def close_db() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


def ensure_fts(conn: sqlite3.Connection) -> None:
    """Create FTS5 virtual table for fast airport/city/country search."""
    conn.executescript("""
        CREATE VIRTUAL TABLE IF NOT EXISTS airports_fts USING fts5(
            iata_code,
            name,
            city,
            country_code,
            country_name,
            content='',
            tokenize='unicode61 remove_diacritics 2'
        );
    """)
    conn.commit()


def rebuild_fts(conn: sqlite3.Connection) -> None:
    """Populate the FTS index from the airports + countries tables."""
    ensure_fts(conn)
    conn.execute("DELETE FROM airports_fts")
    conn.execute("""
        INSERT INTO airports_fts (iata_code, name, city, country_code, country_name)
        SELECT a.iata_code, a.name, a.city, a.country_code,
               COALESCE(c.name, '')
        FROM airports a
        LEFT JOIN countries c ON a.country_code = c.code
    """)
    conn.commit()
