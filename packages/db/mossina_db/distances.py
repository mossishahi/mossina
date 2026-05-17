"""Precompute the airport-to-airport ground-distance graph.

The function is importable so the scraper can call it after airport
data changes. The module is also runnable directly so the deploy can
populate the table without depending on `scripts/`:

    python -m mossina_db.distances

The standalone CLI in `scripts/compute_airport_distances.py` is the
local-development equivalent.
"""

from __future__ import annotations

import logging
import math
import os

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from mossina_db.models import MAX_GROUND_DISTANCE_KM, Airport, AirportDistance

log = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two (lat, lon) points in kilometres.

    Accurate enough for distances <= a few hundred km (sub-percent error
    vs WGS84 geodesic). We only ever care about the <=200km regime here.
    """
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2.0) ** 2
    )
    c = 2.0 * math.asin(min(1.0, math.sqrt(a)))
    return EARTH_RADIUS_KM * c


def recompute_airport_distances(
    session: Session,
    max_km: float = float(MAX_GROUND_DISTANCE_KM),
) -> int:
    """Wipe and rebuild airport_distances for all pairs within max_km.

    Idempotent. Safe to run after every scrape pass.

    Returns the number of pairs written.
    """
    airports = session.execute(
        select(Airport.iata_code, Airport.latitude, Airport.longitude)
        .where(Airport.latitude.is_not(None))
        .where(Airport.longitude.is_not(None))
        .order_by(Airport.iata_code)
    ).all()

    n = len(airports)
    log.info(
        "Recomputing airport distances over %d airports (cutoff %.0f km)...",
        n, max_km,
    )

    rows: list[dict] = []
    for i in range(n):
        ai, lat_i, lon_i = airports[i]
        for j in range(i + 1, n):
            aj, lat_j, lon_j = airports[j]
            d = haversine_km(lat_i, lon_i, lat_j, lon_j)
            if d <= max_km:
                # Canonical order is already guaranteed because we sorted
                # the airports by iata_code and i < j.
                rows.append({"a": ai, "b": aj, "distance_km": d})

    # Atomic refresh: wipe then bulk-insert. price_history holds no
    # references to this table, and routes/fares/etc are decoupled, so a
    # brief empty window is fine -- but we keep it inside one transaction.
    session.execute(text("DELETE FROM airport_distances"))
    if rows:
        session.execute(AirportDistance.__table__.insert(), rows)
    session.commit()

    log.info(
        "airport_distances: %d pairs stored "
        "(%d airports, %.0f km cutoff, %d total combinations).",
        len(rows), n, max_km, n * (n - 1) // 2,
    )
    return len(rows)


def _main() -> None:
    """Module-level runnable: rebuild from DATABASE_URL_SYNC."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    dsn = os.environ.get("DATABASE_URL_SYNC")
    if not dsn:
        raise SystemExit("DATABASE_URL_SYNC is required")
    # Local import keeps the SQLAlchemy engine concerns out of the
    # importable surface used by the scraper.
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(dsn, echo=False, pool_pre_ping=True)
    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        n = recompute_airport_distances(session)
    print(f"rows: {n}")


if __name__ == "__main__":
    _main()
