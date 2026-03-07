"""Background price-update worker.

Can be used in two ways:

1. **Standalone** (for cron / systemd):
       python -m src.update_worker
       python -m src.update_worker --airlines FR
       python -m src.update_worker --origins FMM,NUE

2. **Integrated** (called from serve.py with --schedule):
       from src.update_worker import run_update, start_scheduler
"""

import argparse
import logging
import threading
import time
from datetime import datetime, timedelta

from src.config import setup_logging
from src.db import connect
from src.scraper.ryanair.availability import scrape_availability
from src.scraper.wizzair.schedules import scrape_schedules as w6_scrape

log = logging.getLogger("updater")


def run_update(airlines=None, origins=None, stop_event=None):
    """Run a full price update. Returns a summary dict.

    Args:
        airlines:   set of codes to update, e.g. {"FR", "W6"}. None = all.
        origins:    list of IATA origin codes to limit routes. None = all.
        stop_event: threading.Event to allow graceful cancellation.
    """
    if stop_event is None:
        stop_event = threading.Event()

    run_fr = airlines is None or "FR" in airlines
    run_w6 = airlines is None or "W6" in airlines

    conn = connect()
    t0 = time.time()
    results = {"airlines": [], "errors": [], "stopped": False}

    if run_fr and not stop_event.is_set():
        log.info("[update] Starting Ryanair availability scrape …")
        try:
            def fr_log(done, total, fares, route_ms=0):
                if done % 50 == 0 or done == total:
                    log.info("[update] FR: %d/%d routes, %d prices", done, total, fares)
            scrape_availability(conn, origins=origins, on_progress=fr_log,
                                stop_event=stop_event)
            if stop_event.is_set():
                results["stopped"] = True
                log.info("[update] FR stopped early (data saved)")
            else:
                log.info("[update] FR complete")
            results["airlines"].append("FR")
        except Exception as exc:
            log.exception("[update] FR failed: %s", exc)
            results["errors"].append(f"FR: {exc}")

    if run_w6 and not stop_event.is_set():
        log.info("[update] Starting Wizz Air timetable scrape …")
        try:
            def w6_log(done, total, fares, route_ms=0):
                if done % 20 == 0 or done == total:
                    log.info("[update] W6: %d/%d pairs, %d prices", done, total, fares)
            w6_scrape(conn, days_fresh=0, on_progress=w6_log,
                      stop_event=stop_event)
            if stop_event.is_set():
                results["stopped"] = True
                log.info("[update] W6 stopped early (data saved)")
            else:
                log.info("[update] W6 complete")
            results["airlines"].append("W6")
        except Exception as exc:
            log.exception("[update] W6 failed: %s", exc)
            results["errors"].append(f"W6: {exc}")

    conn.close()
    elapsed = time.time() - t0
    log.info("[update] Finished in %.1f min (%d errors)",
             elapsed / 60, len(results["errors"]))
    return results


def start_scheduler(schedule_time, airlines=None, origins=None,
                    stop_event=None):
    """Start a daemon thread that runs run_update daily at *schedule_time*.

    Args:
        schedule_time: "HH:MM" in local time, e.g. "03:00"
        stop_event:    shared Event; setting it cancels the current run
                       *and* stops the scheduler loop.
    Returns the scheduler thread (already started).
    """
    hour, minute = (int(x) for x in schedule_time.split(":"))
    if stop_event is None:
        stop_event = threading.Event()

    def _loop():
        while not stop_event.is_set():
            now = datetime.now()
            target = now.replace(hour=hour, minute=minute, second=0,
                                 microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            wait_sec = (target - now).total_seconds()
            log.info("[scheduler] Next update at %s (in %.0f min)",
                     target.strftime("%Y-%m-%d %H:%M"), wait_sec / 60)

            if stop_event.wait(wait_sec):
                break

            log.info("[scheduler] Starting scheduled update")
            stop_event.clear()
            run_update(airlines=airlines, origins=origins,
                       stop_event=stop_event)

    t = threading.Thread(target=_loop, name="update-scheduler", daemon=True)
    t.start()
    return t


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Mossina — run a one-off price update",
    )
    parser.add_argument("--airlines", type=str, default=None,
                        help="Comma-separated airline codes (FR,W6). Default: all")
    parser.add_argument("--origins", type=str, default=None,
                        help="Comma-separated origin airports. Default: all")
    args = parser.parse_args()

    airlines = (
        set(a.strip().upper() for a in args.airlines.split(",") if a.strip())
        if args.airlines else None
    )
    origins = (
        [o.strip().upper() for o in args.origins.split(",") if o.strip()]
        if args.origins else None
    )

    stop = threading.Event()
    try:
        run_update(airlines=airlines, origins=origins, stop_event=stop)
    except KeyboardInterrupt:
        log.info("Interrupted — setting stop flag")
        stop.set()


if __name__ == "__main__":
    main()
