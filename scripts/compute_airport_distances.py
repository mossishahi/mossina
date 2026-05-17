"""CLI wrapper for the airport-distance precompute.

Usage:
    DATABASE_URL_SYNC=postgresql+psycopg2://... \
        python scripts/compute_airport_distances.py [--max-km 200]

Idempotent: wipes airport_distances and rebuilds from current airports.
"""

import argparse
import logging
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mossina_db.distances import recompute_airport_distances
from mossina_db.models import MAX_GROUND_DISTANCE_KM


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-km",
        type=float,
        default=MAX_GROUND_DISTANCE_KM,
        help=(
            f"Distance cutoff in km (default: {MAX_GROUND_DISTANCE_KM}). "
            "Pairs farther than this are not stored."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    dsn = os.environ.get("DATABASE_URL_SYNC")
    if not dsn:
        print(
            "ERROR: DATABASE_URL_SYNC is required.",
            file=sys.stderr,
        )
        sys.exit(2)

    engine = create_engine(dsn, echo=False, pool_pre_ping=True)
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    with Session() as session:
        n = recompute_airport_distances(session, max_km=args.max_km)
    print(f"Done. {n} airport pairs stored.")


if __name__ == "__main__":
    main()
