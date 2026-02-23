"""HTTP session with retry / rate-limit logic."""

import logging
import time

import requests

from src.config import HEADERS, MAX_RETRIES, REQUEST_DELAY, RETRY_BACKOFF

log = logging.getLogger("scraper")

session = requests.Session()
session.headers.update(HEADERS)


def api_get(url, params=None):
    """GET with retries and rate-limit backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = RETRY_BACKOFF * attempt
                log.warning("Rate limited (429). Waiting %ds ...", wait)
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                log.debug("404 for %s", url)
                return None
            log.warning("HTTP %d for %s (attempt %d)", resp.status_code, url, attempt)
        except requests.RequestException as exc:
            log.warning("Request error: %s (attempt %d)", exc, attempt)
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)
    log.error("Failed after %d attempts: %s", MAX_RETRIES, url)
    return None
