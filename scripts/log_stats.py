#!/usr/bin/env python3
"""
scripts/log_stats.py

Run immediately after scrape.py completes.
- Records row counts + deltas into a scrape_log table in flights.db
- Sends a rich Telegram message with the stats summary

Environment variables (injected via docker-compose):
    TELEGRAM_BOT_TOKEN  — from @BotFather
    TELEGRAM_CHAT_ID    — your personal or group chat ID

Usage:
    python scripts/log_stats.py
    python scripts/log_stats.py --airline FR
"""

import sqlite3
import argparse
import os
import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "flights.db"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")


# ── Database helpers ──────────────────────────────────────────────────────────

def get_counts(conn: sqlite3.Connection) -> dict:
    tables = ["countries", "airports", "routes", "schedules", "fares"]
    counts = {}
    for table in tables:
        try:
            cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
        except sqlite3.OperationalError:
            counts[table] = 0
    return counts


def ensure_log_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at        TEXT    NOT NULL,
            airline       TEXT,
            countries     INTEGER,
            airports      INTEGER,
            routes        INTEGER,
            schedules     INTEGER,
            fares         INTEGER,
            d_countries   INTEGER,
            d_airports    INTEGER,
            d_routes      INTEGER,
            d_schedules   INTEGER,
            d_fares       INTEGER
        )
    """)
    conn.commit()


def get_previous_counts(conn: sqlite3.Connection, airline: str | None) -> dict | None:
    cur = conn.execute(
        """
        SELECT countries, airports, routes, schedules, fares
        FROM scrape_log
        WHERE airline IS ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (airline,)
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(zip(["countries", "airports", "routes", "schedules", "fares"], row))


# ── Telegram ──────────────────────────────────────────────────────────────────

def send_telegram(message: str):
    """Send a message via Telegram Bot API. Fails silently so it never
    blocks the cron pipeline if Telegram is unreachable."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[log_stats] Telegram not configured — skipping notification.")
        return

    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       message,
        "parse_mode": "HTML",
    }).encode()

    try:
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if not result.get("ok"):
                print(f"[log_stats] Telegram API error: {result}")
    except Exception as e:
        print(f"[log_stats] Telegram send failed (non-fatal): {e}")


def build_telegram_message(
    run_at: str,
    airline: str | None,
    current: dict,
    deltas: dict,
) -> str:
    scope = airline or "ALL airlines"

    def fmt_delta(key):
        d = deltas.get(f"d_{key}")
        if d is None:
            return "—"
        if d > 0:
            return f'<b>+{d}</b>'
        if d < 0:
            return f'<b>{d}</b>'
        return "no change"

    # Readable UTC time
    dt = datetime.fromisoformat(run_at)
    time_str = dt.strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        "✈️  <b>Mossina scrape complete</b>",
        f"🕐  {time_str}",
        f"🔍  Scope: {scope}",
        "",
        "<pre>",
        f"{'Table':<12} {'Total':>8}   {'Change':>10}",
        f"{'-'*36}",
        f"{'Countries':<12} {current['countries']:>8}   {fmt_delta('countries'):>10}",
        f"{'Airports':<12} {current['airports']:>8}   {fmt_delta('airports'):>10}",
        f"{'Routes':<12} {current['routes']:>8}   {fmt_delta('routes'):>10}",
        f"{'Schedules':<12} {current['schedules']:>8}   {fmt_delta('schedules'):>10}",
        f"{'Fares':<12} {current['fares']:>8}   {fmt_delta('fares'):>10}",
        "</pre>",
    ]

    # Add a summary line for notable changes
    new_routes = deltas.get("d_routes") or 0
    new_fares  = deltas.get("d_fares")  or 0
    if new_routes > 0 or new_fares > 0:
        lines.append(f"🆕  {new_routes:+} routes, {new_fares:+} fares since last run")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--airline", default=None, help="FR, W6, or omit for full scrape")
    args = parser.parse_args()

    if not DB_PATH.exists():
        msg = f"[log_stats] ⚠️ Database not found at {DB_PATH}"
        print(msg)
        send_telegram(f"⚠️ <b>Mossina warning</b>\nDatabase file not found after scrape.")
        return

    conn = sqlite3.connect(DB_PATH)
    ensure_log_table(conn)

    current  = get_counts(conn)
    previous = get_previous_counts(conn, args.airline)

    deltas = {}
    for key in current:
        deltas[f"d_{key}"] = (current[key] - previous[key]) if previous else None

    run_at = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO scrape_log
            (run_at, airline,
             countries, airports, routes, schedules, fares,
             d_countries, d_airports, d_routes, d_schedules, d_fares)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_at, args.airline,
            current["countries"], current["airports"], current["routes"],
            current["schedules"], current["fares"],
            deltas["d_countries"], deltas["d_airports"], deltas["d_routes"],
            deltas["d_schedules"], deltas["d_fares"],
        )
    )
    conn.commit()
    conn.close()

    # ── Print to stdout (captured by cron log file) ───────────────────────────
    print(f"\n{'='*55}")
    print(f"  Mossina scrape stats — {run_at}")
    print(f"  Airline scope : {args.airline or 'ALL'}")
    print(f"{'='*55}")
    print(f"  {'Table':<12} {'Total':>8}  {'Change':>8}")
    print(f"  {'-'*34}")
    for table in ["countries", "airports", "routes", "schedules", "fares"]:
        total = current[table]
        d     = deltas[f"d_{table}"]
        d_str = f"+{d}" if d and d > 0 else str(d) if d is not None else "n/a"
        print(f"  {table:<12} {total:>8}  {d_str:>8}")
    print(f"{'='*55}\n")

    # ── Send Telegram notification ────────────────────────────────────────────
    message = build_telegram_message(run_at, args.airline, current, deltas)
    send_telegram(message)
    print("[log_stats] Telegram notification sent.")


if __name__ == "__main__":
    main()