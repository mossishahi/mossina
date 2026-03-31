"""One-time migration: copy data from SQLite (flights.db) to PostgreSQL."""

import os
import sqlite3
import sys

import psycopg2

SQLITE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "flights.db")
PG_DSN = os.environ.get(
    "DATABASE_URL_SYNC",
    "postgresql://mossina:mossina@localhost:5432/mossina",
)


def migrate():
    print(f"SQLite: {SQLITE_PATH}")
    print(f"PostgreSQL: {PG_DSN}")

    sq = sqlite3.connect(SQLITE_PATH)
    sq.row_factory = sqlite3.Row
    pg = psycopg2.connect(PG_DSN)
    cur = pg.cursor()

    # Countries
    rows = sq.execute("SELECT code, name, currency FROM countries").fetchall()
    print(f"Countries: {len(rows)}")
    for r in rows:
        cur.execute(
            "INSERT INTO countries (code, name, currency) VALUES (%s, %s, %s) "
            "ON CONFLICT (code) DO NOTHING",
            (r["code"], r["name"], r["currency"]),
        )
    pg.commit()

    # Airports
    rows = sq.execute(
        "SELECT iata_code, name, city, country_code, latitude, longitude, timezone "
        "FROM airports"
    ).fetchall()
    print(f"Airports: {len(rows)}")
    for r in rows:
        cur.execute(
            "INSERT INTO airports (iata_code, name, city, country_code, latitude, longitude, timezone) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (iata_code) DO NOTHING",
            (r["iata_code"], r["name"], r["city"], r["country_code"],
             r["latitude"], r["longitude"], r["timezone"]),
        )
    pg.commit()

    # Routes
    rows = sq.execute(
        "SELECT origin, destination, airline, is_connecting, new_route, last_seen "
        "FROM routes"
    ).fetchall()
    print(f"Routes: {len(rows)}")
    for r in rows:
        cur.execute(
            "INSERT INTO routes (origin, destination, airline, is_connecting, new_route, last_seen) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (origin, destination, airline) DO NOTHING",
            (r["origin"], r["destination"], r["airline"],
             bool(r["is_connecting"]), bool(r["new_route"]), r["last_seen"]),
        )
    pg.commit()

    # Fares (batch insert for speed)
    count = sq.execute("SELECT COUNT(*) FROM fares").fetchone()[0]
    print(f"Fares: {count} (batch inserting...)")
    batch = []
    batch_size = 5000
    done = 0
    for r in sq.execute(
        "SELECT origin, destination, departure_date, arrival_date, "
        "price, currency, flight_number, scraped_at, airline FROM fares"
    ):
        dep = r["departure_date"][:10] if r["departure_date"] else None
        arr = r["arrival_date"][:10] if r["arrival_date"] else None
        batch.append((
            r["origin"], r["destination"], r["airline"],
            dep, arr, r["price"], r["currency"],
            r["flight_number"], r["scraped_at"],
        ))
        if len(batch) >= batch_size:
            _insert_fares(cur, batch)
            pg.commit()
            done += len(batch)
            batch = []
            sys.stdout.write(f"\r  fares: {done}/{count}")
            sys.stdout.flush()
    if batch:
        _insert_fares(cur, batch)
        pg.commit()
        done += len(batch)
    print(f"\r  fares: {done}/{count} done")

    # Schedules (old schema uses year/month/day instead of departure_date)
    sched_count = sq.execute("SELECT COUNT(*) FROM schedules").fetchone()[0]
    print(f"Schedules: {sched_count} (batch inserting...)")
    batch = []
    done = 0
    for r in sq.execute(
        "SELECT origin, destination, flight_number, year, month, day, "
        "departure_time, arrival_time, airline, scraped_at FROM schedules"
    ):
        y, m, d = r["year"], r["month"], r["day"]
        dep = f"{y}-{int(m):02d}-{int(d):02d}" if y and m and d else None
        dep_t = r["departure_time"] if r["departure_time"] else None
        arr_t = r["arrival_time"] if r["arrival_time"] else None
        batch.append((
            r["origin"], r["destination"], r["flight_number"],
            dep, dep_t, arr_t,
            r["airline"], r["scraped_at"],
        ))
        if len(batch) >= batch_size:
            _insert_schedules(cur, batch)
            pg.commit()
            done += len(batch)
            batch = []
            sys.stdout.write(f"\r  schedules: {done}/{sched_count}")
            sys.stdout.flush()
    if batch:
        _insert_schedules(cur, batch)
        pg.commit()
        done += len(batch)
    print(f"\r  schedules: {done}/{sched_count} done")

    sq.close()
    pg.close()
    print("Migration complete.")


def _insert_fares(cur, batch):
    args = ",".join(
        cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", row).decode()
        for row in batch
    )
    cur.execute(
        "INSERT INTO fares (origin, destination, airline, departure_date, "
        "arrival_date, price, currency, flight_number, scraped_at) "
        f"VALUES {args} ON CONFLICT DO NOTHING"
    )


def _insert_schedules(cur, batch):
    args = ",".join(
        cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", row).decode()
        for row in batch
    )
    cur.execute(
        "INSERT INTO schedules (origin, destination, flight_number, departure_date, "
        "departure_time, arrival_time, airline, scraped_at) "
        f"VALUES {args} ON CONFLICT DO NOTHING"
    )


if __name__ == "__main__":
    migrate()
