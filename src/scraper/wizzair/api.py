"""Wizzair API client with automatic version discovery.

The Wizzair backend lives at be.wizzair.com under a version path that
changes regularly (e.g. /27.45.0/Api). The version is embedded in the
main website's JavaScript and must be extracted at runtime.

Discovery priority:
  1. CLI override / WIZZAIR_API_URL env var
  2. Previously successful URLs from DB (sorted by last success, newest first)
  3. Direct HTTP fetch of wizzair.com homepage (works with PROXY_URL)

All traffic is routed through PROXY_URL when set (needed for servers
blocked by Wizzair's bot protection). Successful/failed URLs are
tracked in the wizzair_api_urls table as a priority queue.
"""

import logging
import random
import re
import threading
import time

import requests

from src.config import MAX_RETRIES, RETRY_BACKOFF, WIZZAIR_API_URL, PROXY_URL

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
_MIN_INTERVAL = 2.0

_override_api_url = ""
_db_conn = None


def set_api_url(url):
    """Allow callers (e.g. CLI) to set the Wizzair API URL at runtime."""
    global _override_api_url
    _override_api_url = url.rstrip("/") if url else ""
    log.info("[W6] Using provided API URL: %s", _override_api_url)


def set_db(conn):
    """Inject the shared DB connection so api.py can read/write the URL queue."""
    global _db_conn
    _db_conn = conn


# ---------------------------------------------------------------------------
# Priority-queue URL store (backed by wizzair_api_urls table)
# ---------------------------------------------------------------------------

def _load_urls_from_db():
    """Return known API URLs sorted by last_success DESC (best first)."""
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
    """Mark a URL as working; insert if new, bump last_success if existing."""
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
    except Exception as exc:
        log.debug("[W6] Could not record URL success: %s", exc)


def _record_failure(url):
    """Mark a URL as failing; insert if new, bump last_failure if existing."""
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
    except Exception as exc:
        log.debug("[W6] Could not record URL failure: %s", exc)


# ---------------------------------------------------------------------------
# URL health check
# ---------------------------------------------------------------------------

_HEALTH_PATH = "asset/map?languageCode=en-gb"


def _get_proxies():
    """Return proxy dict for requests if PROXY_URL is configured."""
    if not PROXY_URL:
        log.debug("[W6] No proxy configured — requests go direct")
        return None
    host = PROXY_URL.split("@")[-1] if "@" in PROXY_URL else PROXY_URL
    log.info("[W6] Using proxy: %s", host)
    return {"http": PROXY_URL, "https": PROXY_URL}


def _probe_url(url):
    """Quick GET to verify an API base URL is alive. Returns True on 200."""
    try:
        full = f"{url.rstrip('/')}/{_HEALTH_PATH}"
        with requests.Session() as s:
            s.headers.update(_HEADERS)
            proxies = _get_proxies()
            if proxies:
                s.proxies.update(proxies)
            resp = s.get(full, timeout=15)
        return resp.status_code == 200
    except requests.RequestException:
        return False


# ---------------------------------------------------------------------------
# Discovery sources
# ---------------------------------------------------------------------------

def _discover_from_homepage():
    """Try to extract apiUrl from the wizzair.com homepage HTML."""
    html_headers = {
        "User-Agent": _HEADERS["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(_HOMEPAGE_URL, headers=html_headers, timeout=30,
                                proxies=_get_proxies())
            resp.raise_for_status()
            match = re.search(r'"apiUrl"\s*:\s*"([^"]+)"', resp.text)
            if match:
                return match.group(1).replace("\\u002F", "/")
            log.warning("[W6] apiUrl not found in HTML (attempt %d/%d)", attempt, MAX_RETRIES)
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
    """Global rate limiter: ensures at least _MIN_INTERVAL between requests."""
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
    """Independent Wizzair API session for thread-safe parallel use.

    Each instance owns its own requests.Session, API base URL, and
    CSRF token -- safe to use from a single thread without locks.
    """

    def __init__(self, worker_id=0, shared_api_base=None, stop_event=None):
        self.worker_id = worker_id
        self._session = None
        self._api_base = shared_api_base or _override_api_url or WIZZAIR_API_URL or None
        self._stop = stop_event

    def _ensure_session(self):
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(_HEADERS)
            proxies = _get_proxies()
            if proxies:
                self._session.proxies.update(proxies)

    def _reset(self):
        self._session = None
        self._api_base = None

    def _discover_api(self):
        # --- Step 1: try known URLs from DB, best first ---
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

        # --- Step 2: try homepage scrape ---
        if self._api_base is None:
            log.info("[W6-w%d] Trying homepage discovery ...", self.worker_id)
            hp_url = _discover_from_homepage()
            if hp_url:
                self._api_base = hp_url
                _record_success(hp_url, source="homepage")

        # --- Give up ---
        if self._api_base is None:
            raise RuntimeError(
                "Could not discover Wizzair API URL. "
                "The site likely blocked this server. Set WIZZAIR_API_URL env var or "
                "pass --wizzair-api-url (run on your laptop to discover it)."
            )
        log.info("[W6-w%d] API base: %s", self.worker_id, self._api_base)

    def _base(self):
        if self._api_base is None:
            self._discover_api()
        return self._api_base

    def _sync_token(self):
        token = self._session.cookies.get("RequestVerificationToken", "")
        if token:
            self._session.headers["X-RequestVerificationToken"] = token

    def _stopped(self):
        return self._stop is not None and self._stop.is_set()

    def post(self, path, payload):
        """POST with retry, backoff, and session-refresh logic."""
        base = self._base()
        url = f"{base}/{path.lstrip('/')}"
        self._ensure_session()
        sess = self._session

        for attempt in range(1, _POST_RETRIES + 1):
            if self._stopped():
                return None
            try:
                _throttle(self._stop)
                if self._stopped():
                    return None
                resp = sess.post(url, json=payload, timeout=20)

                if resp.status_code == 200:
                    self._sync_token()
                    return resp.json()

                if resp.status_code in (429, 502, 503):
                    jitter = random.uniform(0, 5)
                    wait = 8 * attempt + jitter
                    log.warning(
                        "[W6-w%d] %d - backing off %.0fs (attempt %d)",
                        self.worker_id, resp.status_code, wait, attempt,
                    )
                    self._session = None
                    _interruptible_sleep(wait, self._stop)
                    self._ensure_session()
                    sess = self._session
                    continue

                if resp.status_code == 404:
                    return None

                if resp.status_code == 400 and attempt < _POST_RETRIES:
                    log.debug("[W6-w%d] 400 refreshing session", self.worker_id)
                    self._session = None
                    _interruptible_sleep(2, self._stop)
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
                self._session = None
                _interruptible_sleep(RETRY_BACKOFF * attempt + random.uniform(2, 8), self._stop)
                self._ensure_session()
                sess = self._session
                continue

            if attempt < _POST_RETRIES:
                _interruptible_sleep(RETRY_BACKOFF * attempt, self._stop)

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
            if self._stopped():
                return None
            try:
                _interruptible_sleep(1.0, self._stop)
                if self._stopped():
                    return None
                resp = sess.get(url, params=params, timeout=10)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = RETRY_BACKOFF * attempt
                    log.warning("[W6-w%d] 429 waiting %ds", self.worker_id, wait)
                    _interruptible_sleep(wait, self._stop)
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
