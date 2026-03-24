"""Wizzair API client with automatic version discovery.

The Wizzair backend lives at be.wizzair.com under a version path that
changes regularly (e.g. /28.3.0/Api). The version is embedded in the
main website's JavaScript and must be extracted at runtime.

When SCRAPFLY_API_KEY is set, all requests are routed through ScrapFly
which handles Kasada bot protection transparently. Without it, requests
go direct (works from residential IPs / laptops).

Discovery priority:
  1. CLI override / WIZZAIR_API_URL env var
  2. Previously successful URLs from DB (sorted by last success, newest first)
  3. Homepage fetch (direct or via ScrapFly)
"""

import json
import logging
import random
import re
import threading
import time

import requests

from src.config import MAX_RETRIES, RETRY_BACKOFF, WIZZAIR_API_URL

log = logging.getLogger("scraper")

_HOMEPAGE_URL = "https://wizzair.com/"
_SCRAPFLY_URL = "https://api.scrapfly.io/scrape"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://wizzair.com",
    "Referer": "https://wizzair.com/",
}

_POST_RETRIES = 5
_throttle_lock = threading.Lock()
_last_request_time = 0.0
_MIN_INTERVAL = 1.0

_override_api_url = ""
_db_conn = None
_scrapfly_key = ""


def set_api_url(url):
    global _override_api_url
    _override_api_url = url.rstrip("/") if url else ""
    log.info("[W6] Using provided API URL: %s", _override_api_url)


def set_db(conn):
    global _db_conn
    _db_conn = conn


def set_scrapfly_key(key):
    global _scrapfly_key
    _scrapfly_key = key


def _get_scrapfly_key():
    import os
    return _scrapfly_key or os.getenv("SCRAPFLY_API_KEY", "")


# ---------------------------------------------------------------------------
# Priority-queue URL store (backed by wizzair_api_urls table)
# ---------------------------------------------------------------------------

def _load_urls_from_db():
    if _db_conn is None:
        return []
    try:
        rows = _db_conn.execute(
            "SELECT url FROM wizzair_api_urls "
            "ORDER BY last_success DESC NULLS LAST, created_at DESC"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def _record_success(url, source="homepage"):
    if _db_conn is None:
        return
    try:
        _db_conn.execute(
            """INSERT INTO wizzair_api_urls (url, source, last_success, success_count)
               VALUES (?, ?, datetime('now'), 1)
               ON CONFLICT(url) DO UPDATE SET
                   last_success  = datetime('now'),
                   success_count = success_count + 1""",
            (url, source),
        )
        _db_conn.commit()
    except Exception:
        pass


def _record_failure(url):
    if _db_conn is None:
        return
    try:
        _db_conn.execute(
            """INSERT INTO wizzair_api_urls (url, source, last_failure, failure_count)
               VALUES (?, 'unknown', datetime('now'), 1)
               ON CONFLICT(url) DO UPDATE SET
                   last_failure  = datetime('now'),
                   failure_count = failure_count + 1""",
            (url,),
        )
        _db_conn.commit()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# ScrapFly helpers
# ---------------------------------------------------------------------------

def _scrapfly_get(url):
    """GET via ScrapFly. Returns parsed JSON or None."""
    key = _get_scrapfly_key()
    if not key:
        return None
    try:
        resp = requests.get(_SCRAPFLY_URL, params={
            "key": key,
            "url": url,
            "method": "GET",
        }, timeout=30)
        resp.raise_for_status()
        content = resp.json().get("result", {}).get("content", "")
        return json.loads(content) if content else None
    except Exception as exc:
        log.warning("[W6-scrapfly] GET error: %s", exc)
        return None


def _scrapfly_post(url, payload):
    """POST via ScrapFly. Returns parsed JSON or None."""
    key = _get_scrapfly_key()
    if not key:
        return None
    try:
        resp = requests.post(
            _SCRAPFLY_URL,
            params={"key": key, "url": url, "method": "POST"},
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json().get("result", {}).get("content", "")
        return json.loads(content) if content else None
    except Exception as exc:
        log.warning("[W6-scrapfly] POST error: %s", exc)
        return None


# ---------------------------------------------------------------------------
# URL health check + discovery
# ---------------------------------------------------------------------------

_HEALTH_PATH = "asset/map?languageCode=en-gb"


def _probe_url(url):
    full = f"{url.rstrip('/')}/{_HEALTH_PATH}"
    try:
        key = _get_scrapfly_key()
        if key:
            data = _scrapfly_get(full)
            return data is not None and "cities" in data
        resp = requests.get(full, headers=_HEADERS, timeout=15)
        return resp.status_code == 200
    except Exception:
        return False


def _discover_from_homepage():
    key = _get_scrapfly_key()
    if key:
        try:
            resp = requests.get(_SCRAPFLY_URL, params={
                "key": key,
                "url": _HOMEPAGE_URL,
            }, timeout=30)
            resp.raise_for_status()
            html = resp.json().get("result", {}).get("content", "")
            match = re.search(r'"apiUrl"\s*:\s*"([^"]+)"', html)
            if match:
                return match.group(1).replace("\\u002F", "/")
        except Exception as exc:
            log.warning("[W6] ScrapFly homepage fetch failed: %s", exc)

    html_headers = {
        "User-Agent": _HEADERS["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(_HOMEPAGE_URL, headers=html_headers, timeout=30)
            resp.raise_for_status()
            match = re.search(r'"apiUrl"\s*:\s*"([^"]+)"', resp.text)
            if match:
                return match.group(1).replace("\\u002F", "/")
        except requests.RequestException as exc:
            log.warning("[W6] Homepage fetch failed: %s (attempt %d/%d)", exc, attempt, MAX_RETRIES)
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _interruptible_sleep(seconds, stop_event=None):
    if stop_event is None:
        time.sleep(seconds)
    else:
        stop_event.wait(seconds)


def _throttle(stop_event=None):
    global _last_request_time
    with _throttle_lock:
        now = time.time()
        wait = _MIN_INTERVAL - (now - _last_request_time)
        if wait > 0:
            _interruptible_sleep(wait, stop_event)
        _last_request_time = time.time()


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

class WizzairSession:
    """Wizzair API session. Uses ScrapFly when available, direct requests otherwise."""

    def __init__(self, worker_id=0, shared_api_base=None, stop_event=None):
        self.worker_id = worker_id
        self._session = None
        self._api_base = shared_api_base or _override_api_url or WIZZAIR_API_URL or None
        self._stop = stop_event
        self._use_scrapfly = bool(_get_scrapfly_key())

    def _ensure_session(self):
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(_HEADERS)

    def _discover_api(self):
        cached = _load_urls_from_db()
        if cached:
            log.info("[W6-w%d] Trying %d known API URL(s) ...", self.worker_id, len(cached))
        for url in cached:
            if self._stopped():
                break
            if _probe_url(url):
                log.info("[W6-w%d] Cached URL works: %s", self.worker_id, url)
                self._api_base = url
                _record_success(url)
                break
            else:
                log.info("[W6-w%d] Cached URL failed: %s", self.worker_id, url)
                _record_failure(url)

        if self._api_base is None:
            log.info("[W6-w%d] Trying homepage discovery ...", self.worker_id)
            hp_url = _discover_from_homepage()
            if hp_url:
                self._api_base = hp_url
                _record_success(hp_url, source="homepage")

        if self._api_base is None:
            raise RuntimeError(
                "Could not discover Wizzair API URL. "
                "Set WIZZAIR_API_URL env var or pass --wizzair-api-url."
            )
        log.info("[W6-w%d] API base: %s", self.worker_id, self._api_base)

    def _base(self):
        if self._api_base is None:
            self._discover_api()
        return self._api_base

    def _stopped(self):
        return self._stop is not None and self._stop.is_set()

    def post(self, path, payload):
        base = self._base()
        url = f"{base}/{path.lstrip('/')}"

        for attempt in range(1, _POST_RETRIES + 1):
            if self._stopped():
                return None
            try:
                _throttle(self._stop)
                if self._stopped():
                    return None

                if self._use_scrapfly:
                    data = _scrapfly_post(url, payload)
                    if data is not None:
                        return data
                    if attempt < _POST_RETRIES:
                        _interruptible_sleep(RETRY_BACKOFF * attempt, self._stop)
                    continue

                self._ensure_session()
                resp = self._session.post(url, json=payload, timeout=20)

                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (429, 502, 503):
                    wait = 8 * attempt + random.uniform(0, 5)
                    log.warning("[W6-w%d] %d - backing off %.0fs (attempt %d)",
                                self.worker_id, resp.status_code, wait, attempt)
                    _interruptible_sleep(wait, self._stop)
                    continue
                if resp.status_code == 404:
                    return None
                log.warning("[W6-w%d] HTTP %d (attempt %d)",
                            self.worker_id, resp.status_code, attempt)

            except requests.RequestException as exc:
                log.warning("[W6-w%d] POST error: %s (attempt %d)",
                            self.worker_id, exc, attempt)

            if attempt < _POST_RETRIES:
                _interruptible_sleep(RETRY_BACKOFF * attempt, self._stop)

        log.error("[W6-w%d] POST failed after %d attempts: %s",
                  self.worker_id, _POST_RETRIES, url)
        return None

    def get(self, path, params=None):
        base = self._base()
        url = f"{base}/{path.lstrip('/')}"
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{qs}" if "?" not in url else f"{url}&{qs}"

        for attempt in range(1, MAX_RETRIES + 1):
            if self._stopped():
                return None
            try:
                _interruptible_sleep(1.0, self._stop)
                if self._stopped():
                    return None

                if self._use_scrapfly:
                    data = _scrapfly_get(url)
                    if data is not None:
                        return data
                    if attempt < MAX_RETRIES:
                        _interruptible_sleep(RETRY_BACKOFF * attempt, self._stop)
                    continue

                self._ensure_session()
                resp = self._session.get(url, timeout=15)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    _interruptible_sleep(RETRY_BACKOFF * attempt, self._stop)
                    continue
                if resp.status_code == 404:
                    return None
                log.warning("[W6-w%d] HTTP %d (attempt %d)",
                            self.worker_id, resp.status_code, attempt)

            except requests.RequestException as exc:
                log.warning("[W6-w%d] error: %s (attempt %d)",
                            self.worker_id, exc, attempt)
            if attempt < MAX_RETRIES:
                _interruptible_sleep(RETRY_BACKOFF * attempt, self._stop)

        log.error("[W6-w%d] GET/POST failed after %d attempts: %s",
                  self.worker_id, MAX_RETRIES, url)
        return None


# ---------------------------------------------------------------------------
# Module-level functions (used by airports.py and other callers)
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
