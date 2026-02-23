"""Database initialisation and helper utilities."""

import sqlite3
from pathlib import Path

from src.config import DB_PATH

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS countries (
    code        TEXT PRIMARY KEY,
    name        TEXT,
    currency    TEXT
);

CREATE TABLE IF NOT EXISTS airports (
    iata_code       TEXT PRIMARY KEY,
    name            TEXT,
    city            TEXT,
    country_code    TEXT,
    latitude        REAL,
    longitude       REAL,
    timezone        TEXT,
    FOREIGN KEY (country_code) REFERENCES countries(code)
);

CREATE TABLE IF NOT EXISTS routes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    origin          TEXT NOT NULL,
    destination     TEXT NOT NULL,
    airline         TEXT NOT NULL DEFAULT 'FR',
    is_connecting   INTEGER DEFAULT 0,
    new_route       INTEGER DEFAULT 0,
    seasonal_route  INTEGER DEFAULT 0,
    last_seen       TEXT,
    UNIQUE(origin, destination, airline),
    FOREIGN KEY (origin)      REFERENCES airports(iata_code),
    FOREIGN KEY (destination) REFERENCES airports(iata_code)
);

CREATE TABLE IF NOT EXISTS schedules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    origin          TEXT NOT NULL,
    destination     TEXT NOT NULL,
    airline         TEXT NOT NULL DEFAULT 'FR',
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    flight_number   TEXT,
    departure_time  TEXT,
    arrival_time    TEXT,
    carrier         TEXT DEFAULT 'FR',
    scraped_at      TEXT,
    UNIQUE(origin, destination, year, month, day, flight_number),
    FOREIGN KEY (origin)      REFERENCES airports(iata_code),
    FOREIGN KEY (destination) REFERENCES airports(iata_code)
);

CREATE TABLE IF NOT EXISTS fares (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    origin          TEXT NOT NULL,
    destination     TEXT NOT NULL,
    airline         TEXT NOT NULL DEFAULT 'FR',
    departure_date  TEXT,
    arrival_date    TEXT,
    price           REAL,
    currency        TEXT,
    flight_number   TEXT,
    scraped_at      TEXT,
    FOREIGN KEY (origin)      REFERENCES airports(iata_code),
    FOREIGN KEY (destination) REFERENCES airports(iata_code)
);

CREATE INDEX IF NOT EXISTS idx_routes_origin ON routes(origin);
CREATE INDEX IF NOT EXISTS idx_routes_destination ON routes(destination);
CREATE INDEX IF NOT EXISTS idx_schedules_route ON schedules(origin, destination);
CREATE INDEX IF NOT EXISTS idx_fares_route ON fares(origin, destination);
CREATE INDEX IF NOT EXISTS idx_fares_date ON fares(departure_date);
"""


def connect(db_path=None):
    db_path = Path(db_path) if db_path else DB_PATH
    # Migrate from old ryanair.db name if the new name doesn't exist yet
    if not db_path.exists():
        old_path = db_path.parent / "ryanair.db"
        if old_path.exists():
            import shutil
            db_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(old_path), str(db_path))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    _migrate(conn)
    return conn


def _migrate(conn):
    """Add columns / fix constraints that may be missing from older databases."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(routes)")}
    if "last_seen" not in cols:
        conn.execute("ALTER TABLE routes ADD COLUMN last_seen TEXT")
        conn.commit()

    # Check if routes table needs the airline-aware unique constraint.
    # If the old UNIQUE(origin, destination) is still there, we must
    # rebuild the table to use UNIQUE(origin, destination, airline).
    needs_rebuild = "airline" not in cols
    if not needs_rebuild:
        idxs = conn.execute("PRAGMA index_list(routes)").fetchall()
        has_airline_uniq = False
        for idx in idxs:
            idx_name = idx[1]
            idx_cols = [
                r[2] for r in conn.execute(f"PRAGMA index_info({idx_name})")
            ]
            if set(idx_cols) == {"origin", "destination", "airline"} and idx[2]:
                has_airline_uniq = True
                break
        needs_rebuild = not has_airline_uniq

    if needs_rebuild:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS routes_new (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                origin          TEXT NOT NULL,
                destination     TEXT NOT NULL,
                airline         TEXT NOT NULL DEFAULT 'FR',
                is_connecting   INTEGER DEFAULT 0,
                new_route       INTEGER DEFAULT 0,
                seasonal_route  INTEGER DEFAULT 0,
                last_seen       TEXT,
                UNIQUE(origin, destination, airline),
                FOREIGN KEY (origin)      REFERENCES airports(iata_code),
                FOREIGN KEY (destination) REFERENCES airports(iata_code)
            );
            INSERT OR IGNORE INTO routes_new
                (origin, destination, airline, is_connecting, new_route, seasonal_route, last_seen)
                SELECT origin, destination,
                       COALESCE(airline, 'FR'),
                       is_connecting, new_route, seasonal_route, last_seen
                FROM routes;
            DROP TABLE routes;
            ALTER TABLE routes_new RENAME TO routes;
            CREATE INDEX IF NOT EXISTS idx_routes_origin ON routes(origin);
            CREATE INDEX IF NOT EXISTS idx_routes_destination ON routes(destination);
            CREATE INDEX IF NOT EXISTS idx_routes_airline ON routes(airline);
        """)
        conn.commit()

    cols_s = {r[1] for r in conn.execute("PRAGMA table_info(schedules)")}
    if "airline" not in cols_s:
        conn.execute("ALTER TABLE schedules ADD COLUMN airline TEXT NOT NULL DEFAULT 'FR'")
        conn.commit()

    cols_f = {r[1] for r in conn.execute("PRAGMA table_info(fares)")}
    if "airline" not in cols_f:
        conn.execute("ALTER TABLE fares ADD COLUMN airline TEXT NOT NULL DEFAULT 'FR'")
        conn.commit()


def table_counts(conn):
    tables = ["countries", "airports", "routes", "schedules", "fares"]
    counts = {}
    for t in tables:
        try:
            counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            counts[t] = 0
    return counts


def airline_summary(conn):
    """Return dict of airline code -> route count."""
    rows = conn.execute(
        "SELECT airline, COUNT(*) FROM routes GROUP BY airline"
    ).fetchall()
    return {code: cnt for code, cnt in rows}
