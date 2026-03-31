"""Scraper configuration: database URL, HTTP settings, logging."""

import logging
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL_SYNC = os.getenv(
    "DATABASE_URL_SYNC",
    "postgresql+psycopg2://mossina:mossina@localhost:5432/mossina",
)

WIZZAIR_API_URL = os.getenv("WIZZAIR_API_URL", "")
SCRAPFLY_API_KEY = os.getenv("SCRAPFLY_API_KEY", "")

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

_http_session = requests.Session()
_http_session.headers.update(HEADERS)

_log = logging.getLogger("scraper")


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("scraper")


def api_get(url, params=None, delay=None):
    """GET JSON with retries and rate-limit back-off."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(delay if delay is not None else REQUEST_DELAY)
            resp = _http_session.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = RETRY_BACKOFF * attempt
                _log.warning("Rate limited (429). Waiting %ds ...", wait)
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            _log.warning(
                "HTTP %d for %s (attempt %d)", resp.status_code, url, attempt,
            )
        except requests.RequestException as exc:
            _log.warning("Request error: %s (attempt %d)", exc, attempt)
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)
    _log.error("Failed after %d attempts: %s", MAX_RETRIES, url)
    return None
