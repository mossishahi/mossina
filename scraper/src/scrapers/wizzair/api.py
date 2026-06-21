"""Wizzair API client with automatic version discovery.

The Wizzair backend lives at be.wizzair.com under a version path that
changes regularly (e.g. /28.3.0/Api). The version is embedded in the
main website's JavaScript and must be extracted at runtime.

When SCRAPFLY_API_KEY is set, all requests are routed through ScrapFly
which handles Kasada bot protection transparently. Without it, requests
go direct (works from residential IPs / laptops).

Discovery priority:
  1. CLI override / WIZZAIR_API_URL env var
  2. Previously discovered URL (in-memory cache)
  3. Homepage fetch (direct or via ScrapFly)
"""

import json
import logging
import random
import re
import threading
import time

import requests

from src.config import MAX_RETRIES, RETRY_BACKOFF, SCRAPFLY_API_KEY, WIZZAIR_API_URL

log = logging.getLogger("scraper")

_HOMEPAGE_URL = "https://wizzair.com/"
_SCRAPFLY_URL = "https://api.scrapfly.io/scrape"

# wizzair.com is fronted by an AWS WAF CAPTCHA (x-amzn-waf-action: captcha),
# so a plain GET from a datacenter IP returns HTTP 405 + a "Human Verification"
# page instead of the HTML that embeds the versioned API URL. ScrapFly's
# Anti-Scraping-Protection (asp=true) solves the WAF, but ONLY over residential
# IPs -- the default datacenter pool still gets served the CAPTCHA page. We only
# need the homepage once per pass (to read the API version); the be.wizzair.com
# API endpoints themselves are not WAF-protected and are fetched directly.
_SCRAPFLY_HOMEPAGE_TIMEOUT = 180
_SCRAPFLY_HOMEPAGE_ATTEMPTS = 3
_SCRAPFLY_COUNTRY = "de"
_SCRAPFLY_PROXY_POOL = "public_residential_pool"

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
_MIN_INTERVAL = 2.0

_override_api_url = ""
_scrapfly_key = ""
_shared_session = None
_session_lock = threading.Lock()

_cached_api_url = ""
_cached_api_lock = threading.Lock()


def set_api_url(url):
    global _override_api_url
    _override_api_url = url.rstrip("/") if url else ""
    log.info("[W6] Using provided API URL: %s", _override_api_url)


def set_scrapfly_key(key):
    global _scrapfly_key
    _scrapfly_key = key


def _get_scrapfly_key():
    return _scrapfly_key or SCRAPFLY_API_KEY


# ------------------------------------------------------------------
# In-memory URL cache
# ------------------------------------------------------------------

def _cache_url(url):
    global _cached_api_url
    with _cached_api_lock:
        _cached_api_url = url


def _get_cached_url():
    with _cached_api_lock:
        return _cached_api_url


# ------------------------------------------------------------------
# ScrapFly helpers
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# URL discovery
# ------------------------------------------------------------------

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


def _extract_api_url(html):
    """Pull the versioned API base out of homepage HTML, if present."""
    if not html:
        return None
    # The homepage config embeds the base unquoted, e.g. apiUrl:"https://...".
    # Accept optional quotes around the key and single/double quoted values.
    match = re.search(r'["\']?apiUrl["\']?\s*:\s*["\']([^"\']+)["\']', html)
    if match:
        return match.group(1).replace("\\u002F", "/")
    # Fallback: the versioned base is also embedded directly in script src /
    # config as e.g. https://be.wizzair.com/29.3.0/Api
    match = re.search(r'https?:(?:\\u002F|/){2}be\.wizzair\.com(?:\\u002F|/)'
                      r'\d+\.\d+\.\d+(?:\\u002F|/)Api', html)
    if match:
        return match.group(0).replace("\\u002F", "/")
    return None


def _discover_from_homepage():
    key = _get_scrapfly_key()
    if key:
        # asp=true makes ScrapFly solve the AWS WAF CAPTCHA that now guards
        # wizzair.com; without it ScrapFly just relays the challenge page.
        for attempt in range(1, _SCRAPFLY_HOMEPAGE_ATTEMPTS + 1):
            try:
                resp = requests.get(_SCRAPFLY_URL, params={
                    "key": key,
                    "url": _HOMEPAGE_URL,
                    "asp": "true",
                    "proxy_pool": _SCRAPFLY_PROXY_POOL,
                    "country": _SCRAPFLY_COUNTRY,
                }, timeout=_SCRAPFLY_HOMEPAGE_TIMEOUT)
                resp.raise_for_status()
                html = resp.json().get("result", {}).get("content", "")
                api_url = _extract_api_url(html)
                if api_url:
                    return api_url
                log.warning(
                    "[W6] ScrapFly returned no apiUrl (attempt %d/%d)",
                    attempt, _SCRAPFLY_HOMEPAGE_ATTEMPTS,
                )
            except Exception as exc:
                log.warning(
                    "[W6] ScrapFly homepage fetch failed: %s (attempt %d/%d)",
                    exc, attempt, _SCRAPFLY_HOMEPAGE_ATTEMPTS,
                )
            if attempt < _SCRAPFLY_HOMEPAGE_ATTEMPTS:
                time.sleep(RETRY_BACKOFF * attempt)

    html_headers = {
        "User-Agent": _HEADERS["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-GB,en;q=0.9",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(_HOMEPAGE_URL, headers=html_headers, timeout=30)
            resp.raise_for_status()
            api_url = _extract_api_url(resp.text)
            if api_url:
                return api_url
        except requests.RequestException as exc:
            log.warning(
                "[W6] Homepage fetch failed: %s (attempt %d/%d)",
                exc, attempt, MAX_RETRIES,
            )
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)
    return None


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _interruptible_sleep(seconds, stop_event=None):
    if stop_event is None:
        time.sleep(seconds)
    else:
        stop_event.wait(seconds)


class _PerWorkerThrottle:
    """Per-worker rate limiter so each worker waits independently."""
    def __init__(self):
        self._times = {}
        self._lock = threading.Lock()

    def wait(self, worker_id, stop_event=None):
        with self._lock:
            last = self._times.get(worker_id, 0.0)
        now = time.time()
        wait = _MIN_INTERVAL - (now - last)
        if wait > 0:
            _interruptible_sleep(wait, stop_event)
        with self._lock:
            self._times[worker_id] = time.time()

_throttle = _PerWorkerThrottle()


# ------------------------------------------------------------------
# Session
# ------------------------------------------------------------------

class WizzairSession:
    """Wizzair API session with ScrapFly or direct requests."""

    def __init__(self, worker_id=0, shared_api_base=None, stop_event=None):
        self.worker_id = worker_id
        self._session = None
        self._api_base = shared_api_base or _override_api_url or WIZZAIR_API_URL or None
        self._stop = stop_event
        self._use_scrapfly = bool(_get_scrapfly_key())

    def _ensure_session(self):
        global _shared_session
        if self._session is not None:
            return
        if self._use_scrapfly:
            self._session = requests.Session()
            return
        with _session_lock:
            if _shared_session is None:
                _shared_session = requests.Session()
                try:
                    _shared_session.get(_HOMEPAGE_URL, headers={
                        "Accept": "text/html,application/xhtml+xml,*/*",
                        "User-Agent": _HEADERS["User-Agent"],
                    }, timeout=15)
                except requests.RequestException:
                    pass
                _shared_session.headers.update(_HEADERS)
                token = _shared_session.cookies.get("RequestVerificationToken", "")
                if token:
                    _shared_session.headers["X-RequestVerificationToken"] = token
            self._session = _shared_session

    def _discover_api(self):
        cached = _get_cached_url()
        if cached:
            self._api_base = cached
            log.info("[W6-w%d] Using cached URL: %s", self.worker_id, self._api_base)
            return

        log.info("[W6-w%d] Trying homepage discovery ...", self.worker_id)
        hp_url = _discover_from_homepage()
        if hp_url:
            self._api_base = hp_url
            _cache_url(hp_url)

        if self._api_base is None:
            raise RuntimeError(
                "Could not discover Wizzair API URL. wizzair.com is behind an "
                "AWS WAF CAPTCHA, so discovery needs a working SCRAPFLY_API_KEY "
                "(asp bypass) or an explicit WIZZAIR_API_URL "
                "(e.g. https://be.wizzair.com/<version>/Api) override."
            )
        log.info("[W6-w%d] API base: %s", self.worker_id, self._api_base)

    def _base(self):
        if self._api_base is None:
            self._discover_api()
        return self._api_base

    def _sync_token(self):
        if self._session:
            token = self._session.cookies.get("RequestVerificationToken", "")
            if token:
                self._session.headers["X-RequestVerificationToken"] = token

    def _stopped(self):
        return self._stop is not None and self._stop.is_set()

    def post(self, path, payload):
        base = self._base()
        url = f"{base}/{path.lstrip('/')}"

        for attempt in range(1, _POST_RETRIES + 1):
            if self._stopped():
                return None
            try:
                _throttle.wait(self.worker_id, self._stop)
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
                self._sync_token()
                resp = self._session.post(url, json=payload, timeout=20)

                if resp.status_code == 200:
                    self._sync_token()
                    return resp.json()
                if resp.status_code in (429, 502, 503):
                    wait = 8 * attempt + random.uniform(0, 5)
                    log.warning(
                        "[W6-w%d] %d - backing off %.0fs (attempt %d)",
                        self.worker_id, resp.status_code, wait, attempt,
                    )
                    _interruptible_sleep(wait, self._stop)
                    continue
                if resp.status_code == 404:
                    return None
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
                _interruptible_sleep(RETRY_BACKOFF * attempt, self._stop)

        log.error(
            "[W6-w%d] POST failed after %d attempts: %s",
            self.worker_id, _POST_RETRIES, url,
        )
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
                log.warning(
                    "[W6-w%d] HTTP %d (attempt %d)",
                    self.worker_id, resp.status_code, attempt,
                )

            except requests.RequestException as exc:
                log.warning(
                    "[W6-w%d] error: %s (attempt %d)",
                    self.worker_id, exc, attempt,
                )
            if attempt < MAX_RETRIES:
                _interruptible_sleep(RETRY_BACKOFF * attempt, self._stop)

        log.error(
            "[W6-w%d] GET failed after %d attempts: %s",
            self.worker_id, MAX_RETRIES, url,
        )
        return None


# ------------------------------------------------------------------
# Module-level convenience functions
# ------------------------------------------------------------------
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
