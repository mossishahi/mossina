"""Wizzair API client with automatic version discovery.

The Wizzair backend lives at be.wizzair.com under a version path that
changes regularly (e.g. /27.45.0/Api). The version is embedded in the
main website's JavaScript and must be extracted at runtime.

Provides both a stateless WizzairSession class for parallel scraping
and legacy module-level functions for simple single-threaded use.
"""

import logging
import random
import re
import threading
import time

import requests

from src.config import MAX_RETRIES, RETRY_BACKOFF

log = logging.getLogger("scraper")

_HOMEPAGE_URL = "https://wizzair.com/"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://wizzair.com",
    "Referer": "https://wizzair.com/",
}

_POST_RETRIES = 8
_POST_DELAY = 0.4

_throttle_lock = threading.Lock()
_last_request_time = 0.0
_MIN_INTERVAL = 0.5


def _throttle():
    """Global rate limiter: ensures at least _MIN_INTERVAL between requests."""
    global _last_request_time
    with _throttle_lock:
        now = time.time()
        wait = _MIN_INTERVAL - (now - _last_request_time)
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.time()


class WizzairSession:
    """Independent Wizzair API session for thread-safe parallel use.

    Each instance owns its own requests.Session, API base URL, and
    CSRF token -- safe to use from a single thread without locks.
    """

    def __init__(self, worker_id=0, shared_api_base=None):
        self.worker_id = worker_id
        self._session = None
        self._api_base = shared_api_base

    def _ensure_session(self):
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(_HEADERS)

    def _reset(self):
        self._session = None
        self._api_base = None

    def _discover_api(self):
        self._ensure_session()
        resp = self._session.get(
            _HOMEPAGE_URL, headers={"Accept": "text/html"}, timeout=30
        )
        resp.raise_for_status()
        match = re.search(r'"apiUrl"\s*:\s*"([^"]+)"', resp.text)
        if match:
            self._api_base = match.group(1).replace("\\u002F", "/")
        if self._api_base is None:
            raise RuntimeError("Could not discover Wizzair API URL")
        log.debug("[W6-w%d] API base: %s", self.worker_id, self._api_base)

    def _base(self):
        if self._api_base is None:
            self._discover_api()
        return self._api_base

    def _sync_token(self):
        token = self._session.cookies.get("RequestVerificationToken", "")
        if token:
            self._session.headers["X-RequestVerificationToken"] = token

    def post(self, path, payload):
        """POST with retry, backoff, and session-refresh logic."""
        base = self._base()
        url = f"{base}/{path.lstrip('/')}"
        self._ensure_session()
        sess = self._session

        for attempt in range(1, _POST_RETRIES + 1):
            try:
                _throttle()
                resp = sess.post(url, json=payload, timeout=30)

                if resp.status_code == 200:
                    self._sync_token()
                    return resp.json()

                if resp.status_code in (429, 503):
                    jitter = random.uniform(0, 5)
                    wait = 8 * attempt + jitter
                    log.warning(
                        "[W6-w%d] %d - backing off %.0fs (attempt %d)",
                        self.worker_id, resp.status_code, wait, attempt,
                    )
                    self._session = None
                    time.sleep(wait)
                    self._ensure_session()
                    sess = self._session
                    continue

                if resp.status_code == 404:
                    return None

                if resp.status_code == 400 and attempt < _POST_RETRIES:
                    log.debug("[W6-w%d] 400 refreshing session", self.worker_id)
                    self._session = None
                    time.sleep(2)
                    self._ensure_session()
                    sess = self._session
                    continue

                log.warning(
                    "[W6-w%d] HTTP %d (attempt %d)",
                    self.worker_id, resp.status_code, attempt,
                )
            except requests.RequestException as exc:
                log.warning(
                    "[W6-w%d] POST error: %s (attempt %d)",
                    self.worker_id, exc, attempt,
                )

            if attempt < _POST_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)

        log.error("[W6-w%d] POST failed after %d attempts: %s",
                  self.worker_id, _POST_RETRIES, url)
        return None

    def get(self, path, params=None):
        """GET with retry logic."""
        base = self._base()
        url = f"{base}/{path.lstrip('/')}"
        self._ensure_session()
        sess = self._session

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                time.sleep(1.0)
                resp = sess.get(url, params=params, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = RETRY_BACKOFF * attempt
                    log.warning("[W6-w%d] 429 waiting %ds", self.worker_id, wait)
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    return None
                log.warning("[W6-w%d] HTTP %d (attempt %d)",
                            self.worker_id, resp.status_code, attempt)
            except requests.RequestException as exc:
                log.warning("[W6-w%d] error: %s (attempt %d)",
                            self.worker_id, exc, attempt)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
        log.error("[W6-w%d] GET failed after %d attempts: %s",
                  self.worker_id, MAX_RETRIES, url)
        return None


# ---------------------------------------------------------------------------
# Legacy module-level functions (used by airports.py and other single-thread
# callers). Delegates to a default WizzairSession instance.
# ---------------------------------------------------------------------------
_default_session = None


def _get_default():
    global _default_session
    if _default_session is None:
        _default_session = WizzairSession(worker_id=0)
    return _default_session


def get_api_base():
    return _get_default()._base()


def wizzair_get(path, params=None):
    return _get_default().get(path, params)


def wizzair_post(path, payload):
    return _get_default().post(path, payload)
