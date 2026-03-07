"""HTTP session with retry / rate-limit logic."""

import logging
import time

import requests

from src.config import HEADERS, MAX_RETRIES, REQUEST_DELAY, RETRY_BACKOFF

log = logging.getLogger("scraper")

session = requests.Session()
session.headers.update(HEADERS)


def _interruptible_sleep(seconds, stop_event=None):
    """Sleep that returns early when stop_event is set."""
    if stop_event is None:
        time.sleep(seconds)
        return
    stop_event.wait(seconds)


def api_get(url, params=None, delay=None, stop_event=None):
    """GET with retries and rate-limit backoff.

    If *stop_event* is provided and becomes set, the function abandons
    remaining retries and returns ``None`` immediately.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        if stop_event and stop_event.is_set():
            return None
        try:
            _interruptible_sleep(
                delay if delay is not None else REQUEST_DELAY, stop_event)
            if stop_event and stop_event.is_set():
                return None
            resp = session.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = RETRY_BACKOFF * attempt
                log.warning("Rate limited (429). Waiting %ds ...", wait)
                _interruptible_sleep(wait, stop_event)
                continue
            if resp.status_code == 404:
                log.debug("404 for %s", url)
                return None
            log.warning("HTTP %d for %s (attempt %d)", resp.status_code, url, attempt)
        except requests.RequestException as exc:
            log.warning("Request error: %s (attempt %d)", exc, attempt)
        if attempt < MAX_RETRIES:
            _interruptible_sleep(RETRY_BACKOFF * attempt, stop_event)
    log.error("Failed after %d attempts: %s", MAX_RETRIES, url)
    return None
