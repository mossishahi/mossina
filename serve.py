#!/usr/bin/env python3
"""Mossina flight network application server.

This is the main entry point for the application.  It serves the
interactive route-network visualization and exposes API endpoints for
live data operations (price updates, etc.).

Usage:
    python serve.py                        # http://127.0.0.1:8080
    python serve.py --port 9000            # custom port
    python serve.py --host 0.0.0.0         # allow external access
    python serve.py --schedule 03:00       # daily auto-update at 3 AM

Routes:
    GET  /                     -> rendered route-network page (live from DB)
    GET  /api/update-prices    -> SSE stream that scrapes fares + streams progress
    GET  /api/stop-update      -> stops a running price update gracefully
"""

import argparse
import json
import logging
import threading
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlparse

from src.config import setup_logging
from src.db import avg_fetch_time, connect
from src.scraper.ryanair.availability import scrape_availability
from src.scraper.wizzair.schedules import scrape_schedules as w6_scrape
from src.viz.network_graph import build_network_html

log = setup_logging()

_stop_event = threading.Event()
_conn_lock = threading.Lock()
_page_conn = None


def _get_page_conn():
    """Connection for quick read-only page renders (reused, single-threaded)."""
    global _page_conn
    with _conn_lock:
        if _page_conn is None:
            _page_conn = connect()
        return _page_conn


class AppHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        if path == "/":
            self._serve_page()
        elif path == "/api/update-prices":
            qs = parse_qs(parsed.query)
            raw = qs.get("origins", [None])[0]
            origins = (
                [o.strip().upper() for o in raw.split(",") if o.strip()]
                if raw else None
            )
            raw_al = qs.get("airlines", [None])[0]
            airlines = (
                set(a.strip().upper() for a in raw_al.split(",") if a.strip())
                if raw_al else None
            )
            self._sse_update(origins, airlines)
        elif path == "/api/stop-update":
            self._stop_update()
        else:
            self.send_error(404)

    # ── Page ────────────────────────────────────────────────────────────

    def _serve_page(self):
        conn = _get_page_conn()
        try:
            html = build_network_html(conn, output_path=None)
        except Exception:
            log.exception("Failed to render page")
            self.send_error(500, "Failed to render page")
            return

        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    # ── Stop ────────────────────────────────────────────────────────────

    def _stop_update(self):
        _stop_event.set()
        log.info("Stop requested — scrapers will finish current route and save")
        body = json.dumps({"status": "stopping"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    # ── SSE price update ────────────────────────────────────────────────

    def _sse_event(self, event, payload):
        msg = f"event: {event}\ndata: {json.dumps(payload)}\n\n"
        self.wfile.write(msg.encode())
        self.wfile.flush()

    def _sse_update(self, origins, airlines=None):
        _stop_event.clear()

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        conn = connect()
        errors = []
        was_stopped = False
        run_fr = airlines is None or "FR" in airlines
        run_w6 = airlines is None or "W6" in airlines

        if run_fr and not _stop_event.is_set():
            fr_avg = avg_fetch_time(conn, "FR")
            self._sse_event("start", {
                "airline": "FR", "name": "Ryanair",
                "avg_route_ms": round(fr_avg, 0) if fr_avg else None,
            })
            try:
                def fr_cb(done, total, fares, route_ms=0):
                    self._sse_event("progress", {
                        "airline": "FR", "done": done, "total": total,
                        "fares": fares, "route_ms": round(route_ms, 0),
                    })
                scrape_availability(conn, origins=origins, on_progress=fr_cb,
                                    stop_event=_stop_event)
                if _stop_event.is_set():
                    was_stopped = True
                    self._sse_event("stopped", {"airline": "FR"})
                else:
                    self._sse_event("done", {"airline": "FR"})
            except Exception as exc:
                log.exception("Ryanair scrape failed")
                errors.append(f"FR: {exc}")
                self._sse_event("error", {"airline": "FR", "message": str(exc)})

        if run_w6 and not _stop_event.is_set():
            w6_avg = avg_fetch_time(conn, "W6")
            self._sse_event("start", {
                "airline": "W6", "name": "Wizz Air",
                "avg_route_ms": round(w6_avg, 0) if w6_avg else None,
            })
            try:
                def w6_cb(done, total, fares, route_ms=0):
                    self._sse_event("progress", {
                        "airline": "W6", "done": done, "total": total,
                        "fares": fares, "route_ms": round(route_ms, 0),
                    })
                w6_scrape(conn, days_fresh=0, on_progress=w6_cb,
                          stop_event=_stop_event)
                if _stop_event.is_set():
                    was_stopped = True
                    self._sse_event("stopped", {"airline": "W6"})
                else:
                    self._sse_event("done", {"airline": "W6"})
            except Exception as exc:
                log.exception("Wizz Air scrape failed")
                errors.append(f"W6: {exc}")
                self._sse_event("error", {"airline": "W6", "message": str(exc)})

        self._sse_event("complete", {
            "success": len(errors) == 0,
            "stopped": was_stopped,
            "errors": errors,
        })
        conn.close()

    # ── Logging ─────────────────────────────────────────────────────────

    def log_message(self, fmt, *args):
        log.debug(fmt, *args)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def main():
    parser = argparse.ArgumentParser(
        description="Mossina — flight route network application",
    )
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--no-browser", action="store_true",
                        help="Don't open the browser automatically")
    parser.add_argument("--schedule", type=str, default=None, metavar="HH:MM",
                        help="Run automatic daily price update at this time "
                             "(e.g. 03:00)")
    args = parser.parse_args()

    _get_page_conn()
    log.info("Database connected")

    if args.schedule:
        from src.update_worker import start_scheduler
        start_scheduler(args.schedule, stop_event=_stop_event)
        log.info("Scheduler active — daily update at %s", args.schedule)

    server = ThreadedHTTPServer((args.host, args.port), AppHandler)
    url = f"http://{args.host}:{args.port}"
    log.info("Mossina running at %s", url)

    if not args.no_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        _stop_event.set()
        server.shutdown()
        if _page_conn:
            _page_conn.close()


if __name__ == "__main__":
    main()
