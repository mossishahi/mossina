#!/usr/bin/env python3
"""CLI entry point for generating visualisations.

Usage:
    python visualize.py                        # default graph
    python visualize.py --output my_graph.html # custom output path
    python visualize.py --open                 # open in browser after build
"""

import argparse
import webbrowser

from src.config import DB_PATH, OUTPUT_DIR, setup_logging
from src.db import connect
from src.viz.network_graph import build_network_html

log = setup_logging()


def main():
    parser = argparse.ArgumentParser(description="Visualise the flight route network.")
    parser.add_argument("--db", type=str, default=None, help="Path to SQLite database")
    parser.add_argument("--output", "-o", type=str, default=None, help="Output HTML path")
    parser.add_argument("--open", action="store_true", help="Open in browser after build")
    args = parser.parse_args()

    db_path = DB_PATH if not args.db else __import__("pathlib").Path(args.db)
    out_path = args.output or str(OUTPUT_DIR / "route_network.html")

    conn = connect(db_path)
    try:
        path = build_network_html(conn, out_path)
        if args.open:
            webbrowser.open(f"file://{path.resolve()}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
