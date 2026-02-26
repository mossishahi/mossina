"""Shared configuration: paths, constants, logging setup."""

import logging
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Where scraped data / database live.
# By default this is the local "data" directory inside the repo, but it can be
# overridden so that a pre-populated data folder can live anywhere on disk.
#
# Env vars:
# - MOSSINA_DATA_DIR  -> directory containing flights.db and other data files
# - MOSSINA_DB_PATH   -> full path to flights.db (overrides MOSSINA_DATA_DIR)
# - MOSSINA_OUTPUT_DIR -> directory where visualisations are written
_default_data_dir = PROJECT_ROOT / "data"
DATA_DIR = Path(os.getenv("MOSSINA_DATA_DIR", _default_data_dir))

_default_db_path = DATA_DIR / "flights.db"
DB_PATH = Path(os.getenv("MOSSINA_DB_PATH", _default_db_path))

_default_output_dir = PROJECT_ROOT / "output"
OUTPUT_DIR = Path(os.getenv("MOSSINA_OUTPUT_DIR", _default_output_dir))

REQUEST_DELAY = 1.5
MAX_RETRIES = 3
RETRY_BACKOFF = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("scraper")
