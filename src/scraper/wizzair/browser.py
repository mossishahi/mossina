"""Session bootstrap via Firecrawl for Wizzair API access.

Uses Firecrawl to load wizzair.com (bypasses WAF/bot protection),
extracts the CSRF token and session cookies, then injects them into
requests.Session instances so POST endpoints work.

Requires FIRECRAWL_API_KEY env var.
"""

import logging
import os
import re

import requests

log = logging.getLogger("scraper")

_FIRECRAWL_URL = "https://api.firecrawl.dev/v2/scrape"
_WIZZAIR_URL = "https://wizzair.com/en-gb"

_cookies = None
_csrf_token = None


def _get_api_key():
    return os.getenv("FIRECRAWL_API_KEY", "")


def bootstrap():
    """Load wizzair.com via Firecrawl, extract cookies and CSRF token.

    Returns (cookies_dict, csrf_token) or (None, None) on failure.
    """
    global _cookies, _csrf_token

    if _cookies is not None:
        return _cookies, _csrf_token

    api_key = _get_api_key()
    if not api_key:
        return None, None

    log.info("[W6-firecrawl] Bootstrapping session via Firecrawl ...")
    try:
        resp = requests.post(
            _FIRECRAWL_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "url": _WIZZAIR_URL,
                "formats": ["html"],
                "waitFor": 8000,
                "timeout": 60000,
                "actions": [
                    {"type": "wait", "milliseconds": 3000},
                    {
                        "type": "executeJavascript",
                        "script": "return document.cookie",
                    },
                ],
            },
            timeout=90,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            log.warning("[W6-firecrawl] Scrape failed: %s", data.get("error"))
            return None, None

        cookie_str = (
            data.get("data", {})
            .get("actions", {})
            .get("javascriptReturns", [{}])[0]
            .get("value", "")
        )

        _cookies = {}
        for part in cookie_str.split(";"):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                _cookies[k.strip()] = v.strip()

        _csrf_token = _cookies.get("RequestVerificationToken", "")
        log.info(
            "[W6-firecrawl] Got %d cookies, CSRF token: %s...",
            len(_cookies),
            _csrf_token[:10] if _csrf_token else "none",
        )
        return _cookies, _csrf_token

    except Exception as exc:
        log.warning("[W6-firecrawl] Bootstrap failed: %s", exc)
        return None, None


def inject_session(session):
    """Inject Firecrawl-obtained cookies and CSRF token into a requests.Session."""
    cookies, token = bootstrap()
    if not cookies:
        return False
    for k, v in cookies.items():
        session.cookies.set(k, v)
    if token:
        session.headers["X-RequestVerificationToken"] = token
    return True


def reset():
    """Force re-bootstrap on next call."""
    global _cookies, _csrf_token
    _cookies = None
    _csrf_token = None
