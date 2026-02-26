"""Interactive directed-graph visualisation of the flight route network.

Nodes  = airports (sized by number of outbound routes)
Edges  = directed routes between airports
Colors = grouped by country
"""

import json
import logging
import math
import sqlite3
from pathlib import Path

import requests

log = logging.getLogger("scraper")

_EUR_RATE_URL = "https://open.er-api.com/v6/latest/EUR"


def _fetch_eur_rates(currencies):
    """Return a list of rates (units-per-1-EUR) aligned with the currencies list."""
    rates = [1.0] * len(currencies)
    try:
        resp = requests.get(_EUR_RATE_URL, timeout=10)
        resp.raise_for_status()
        api_rates = resp.json().get("rates", {})
        for i, cur in enumerate(currencies):
            if cur in api_rates:
                rates[i] = round(api_rates[cur], 6)
            else:
                log.warning("No EUR rate for %s, prices will show as-is", cur)
        log.info("Fetched EUR exchange rates for %d currencies", len(currencies))
    except Exception as exc:
        log.warning("Could not fetch exchange rates: %s -- prices will show as-is", exc)
    return rates


COUNTRY_PALETTE = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
    "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5",
    "#393b79", "#5254a3", "#6b6ecf", "#9c9ede", "#637939",
    "#8ca252", "#b5cf6b", "#cedb9c", "#8c6d31", "#bd9e39",
    "#e7ba52", "#e7cb94", "#843c39", "#ad494a", "#d6616b",
    "#e7969c", "#7b4173", "#a55194", "#ce6dbd", "#de9ed6",
]


_COUNTRY_NAMES = {
    "al": "Albania", "at": "Austria", "ba": "Bosnia and Herzegovina",
    "be": "Belgium", "bg": "Bulgaria", "ch": "Switzerland", "cy": "Cyprus",
    "cz": "Czech Republic", "de": "Germany", "dk": "Denmark", "ee": "Estonia",
    "es": "Spain", "fi": "Finland", "fr": "France", "gb": "United Kingdom",
    "gr": "Greece", "hr": "Croatia", "hu": "Hungary", "ie": "Ireland",
    "il": "Israel", "is": "Iceland", "it": "Italy", "jo": "Jordan",
    "lt": "Lithuania", "lu": "Luxembourg", "lv": "Latvia", "ma": "Morocco",
    "me": "Montenegro", "mk": "North Macedonia", "mt": "Malta",
    "nl": "Netherlands", "no": "Norway", "pl": "Poland", "pt": "Portugal",
    "ro": "Romania", "rs": "Serbia", "se": "Sweden", "si": "Slovenia",
    "sk": "Slovakia", "tr": "Turkey", "ua": "Ukraine",
}


def _load_graph_data(conn):
    country_names = {}
    for row in conn.execute("SELECT code, name FROM countries"):
        if row[1]:
            country_names[row[0]] = row[1]

    airports = {}
    for row in conn.execute(
        "SELECT iata_code, name, city, country_code, latitude, longitude FROM airports"
    ):
        iata, name, city, cc, lat, lon = row
        cname = country_names.get(cc) or _COUNTRY_NAMES.get(cc, cc.upper() if cc else "")
        airports[iata] = {
            "name": name, "city": city, "country": cc,
            "lat": lat, "lon": lon,
            "country_name": cname,
        }

    routes_raw = conn.execute("SELECT origin, destination, airline FROM routes").fetchall()
    routes = [(r[0], r[1]) for r in routes_raw]

    route_airlines = {}
    for origin, dest, airline in routes_raw:
        key = f"{origin}-{dest}"
        if key not in route_airlines:
            route_airlines[key] = []
        if airline not in route_airlines[key]:
            route_airlines[key].append(airline)

    degree = {}
    for origin, dest in routes:
        degree[origin] = degree.get(origin, 0) + 1

    avail = {}
    for origin, dest, dep in conn.execute(
        "SELECT origin, destination, departure_date FROM fares "
        "WHERE departure_date IS NOT NULL"
    ):
        day = dep[:10] if dep else None
        if not day:
            continue
        key = f"{origin}-{dest}"
        if key not in avail:
            avail[key] = []
        if day not in avail[key]:
            avail[key].append(day)
    for origin, dest, y, m, d in conn.execute(
        "SELECT origin, destination, year, month, day FROM schedules"
    ):
        if not (y and m and d):
            continue
        day = f"{y}-{m:02d}-{d:02d}"
        key = f"{origin}-{dest}"
        if key not in avail:
            avail[key] = []
        if day not in avail[key]:
            avail[key].append(day)
    for k in avail:
        avail[k].sort()

    from datetime import date as _date
    base_date = _date(2026, 1, 1)
    currencies = []
    cur_idx = {}
    fares = {}
    for origin, dest, dep, price, currency, airline in conn.execute(
        """SELECT origin, destination, substr(departure_date, 1, 10),
                  MIN(price), currency, airline
           FROM fares
           WHERE departure_date >= date('now') AND price > 0
           GROUP BY origin, destination, substr(departure_date, 1, 10)
           ORDER BY origin, destination, departure_date"""
    ):
        if not dep or price is None:
            continue
        if currency not in cur_idx:
            cur_idx[currency] = len(currencies)
            currencies.append(currency)
        key = f"{origin}-{dest}"
        if key not in fares:
            fares[key] = []
        try:
            d = _date.fromisoformat(dep)
            offset = (d - base_date).days
        except ValueError:
            continue
        p = round(price, 2)
        if p == int(p):
            p = int(p)
        fares[key].append([offset, p, cur_idx[currency], airline])

    eur_rates = _fetch_eur_rates(currencies)

    fare_data = {"_c": currencies, "_r": eur_rates}
    fare_data.update(fares)

    return airports, routes, degree, avail, route_airlines, fare_data


def _assign_colors(airports):
    countries = sorted(set(a["country"] for a in airports.values() if a["country"]))
    color_map = {}
    for i, cc in enumerate(countries):
        color_map[cc] = COUNTRY_PALETTE[i % len(COUNTRY_PALETTE)]
    return color_map


def build_network_html(conn, output_path):
    """Build a standalone interactive HTML file with the route network."""
    airports, routes, degree, avail, route_airlines, fare_data = _load_graph_data(conn)
    color_map = _assign_colors(airports)

    nodes_with_routes = set()
    for origin, dest in routes:
        nodes_with_routes.add(origin)
        nodes_with_routes.add(dest)

    nodes_js = []
    for iata, info in airports.items():
        if iata not in nodes_with_routes:
            continue
        deg = degree.get(iata, 0)
        size = 4 + math.sqrt(deg) * 3
        color = color_map.get(info["country"], "#888888")
        label = iata
        title = (
            f"<b>{info['name']}</b> ({iata})<br>"
            f"City: {info['city']}<br>"
            f"Country: {info['country'].upper()}<br>"
            f"Routes: {deg}<br>"
            f"Lat: {info['lat']}, Lon: {info['lon']}"
        )
        x = (info["lon"] or 0) * 12
        y = -(info["lat"] or 0) * 12
        nodes_js.append({
            "id": iata, "label": label, "title": title,
            "size": round(size, 1), "color": color,
            "x": round(x, 1), "y": round(y, 1),
            "country": info["country"],
            "lat": info["lat"] or 0,
            "lon": info["lon"] or 0,
            "name": info["name"],
            "city": info["city"],
            "country_name": info["country_name"],
        })

    edges_js = []
    seen_edges = set()
    for origin, dest in routes:
        edge_key = f"{origin}-{dest}"
        if origin in airports and dest in airports and edge_key not in seen_edges:
            airlines = route_airlines.get(edge_key, [])
            edges_js.append({"from": origin, "to": dest, "airlines": airlines})
            seen_edges.add(edge_key)

    country_legend = {}
    for iata, info in airports.items():
        cc = info["country"]
        if cc and cc not in country_legend:
            country_legend[cc] = color_map.get(cc, "#888")

    airline_meta = {}
    all_codes = set()
    for e in edges_js:
        for al in e.get("airlines", []):
            all_codes.add(al)
    _airline_info = {
        "FR": {"name": "Ryanair", "color": "#003399"},
        "W6": {"name": "Wizz Air", "color": "#c6007e"},
    }
    for code in sorted(all_codes):
        info = _airline_info.get(code, {"name": code, "color": "#888888"})
        cnt = sum(1 for e in edges_js if code in e.get("airlines", []))
        airline_meta[code] = {"name": info["name"], "color": info["color"], "routes": cnt}

    html = _TEMPLATE.replace("__NODES__", json.dumps(nodes_js))
    html = html.replace("__EDGES__", json.dumps(edges_js))
    html = html.replace("__LEGEND__", json.dumps(country_legend))
    html = html.replace("__NODE_COUNT__", str(len(nodes_js)))
    html = html.replace("__EDGE_COUNT__", str(len(edges_js)))
    html = html.replace("__AVAIL__", json.dumps(avail, separators=(",", ":")))
    html = html.replace("__AIRLINE_META__", json.dumps(airline_meta))
    html = html.replace("__FARE_DATA__", json.dumps(fare_data, separators=(",", ":")))

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    log.info("Graph written to %s (%d nodes, %d edges)", output_path, len(nodes_js), len(edges_js))
    return output_path


_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Flight Route Network</title>
<script src="https://unpkg.com/globe.gl"></script>
<script src="https://unpkg.com/topojson-client@3"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: #000; color: #c9d1d9; overflow: hidden;
  }
  #globeViz { width: 100vw; height: 100vh; }

  #left-col {
    position: fixed; top: 12px; left: 12px; z-index: 1000;
    width: 420px; min-width: 380px; display: flex; flex-direction: column; gap: 8px;
    pointer-events: none; max-height: calc(100vh - 24px);
    overflow-y: auto; overflow-x: hidden;
  }
  #left-col::-webkit-scrollbar { width: 5px; }
  #left-col::-webkit-scrollbar-track { background: transparent; }
  #left-col::-webkit-scrollbar-thumb { background: rgba(88,166,255,0.25); border-radius: 3px; }
  #left-col::-webkit-scrollbar-thumb:hover { background: rgba(88,166,255,0.5); }
  #left-col > * { pointer-events: auto; flex-shrink: 0; }
  #left-col-resize {
    position: absolute; top: 0; right: -5px; width: 10px; height: 100%;
    cursor: col-resize; pointer-events: auto; z-index: 11;
  }
  #left-col-resize::after {
    content: ""; position: absolute; top: 50%; right: 4px;
    width: 3px; height: 28px; transform: translateY(-50%);
    background: rgba(88,166,255,0.25); border-radius: 2px;
    transition: background 0.15s;
  }
  #left-col-resize:hover::after { background: rgba(88,166,255,0.6); }

  #panel {
    display: flex; flex-direction: column;
    background: rgba(22,27,34,0.94); border: 1px solid #30363d;
    border-radius: 10px; overflow: hidden; backdrop-filter: blur(10px);
  }
  #panel-top { padding: 12px 12px 0; flex-shrink: 0; }
  #panel-top h2 { font-size: 14px; margin-bottom: 6px; color: #58a6ff; }
  .stat { font-size: 11px; color: #8b949e; margin-bottom: 2px; }
  #status {
    font-size: 10px; color: #58a6ff; margin: 6px 0 4px;
    padding: 4px 7px; background: #161b22; border-radius: 4px;
  }
  #search-wrap { position: relative; }
  #search {
    width: 100%; padding: 6px 8px; margin: 4px 0 0;
    background: #161b22; border: 1px solid #30363d; border-radius: 5px;
    color: #c9d1d9; font-size: 12px; outline: none;
  }
  #search:focus { border-color: #58a6ff; }
  #search::placeholder { color: #484f58; }
  #suggestions {
    position: absolute; left: 0; right: 0; top: 100%;
    background: #161b22; border: 1px solid #30363d; border-radius: 0 0 5px 5px;
    max-height: 160px; overflow-y: auto; display: none; z-index: 30;
  }
  .sug { padding: 5px 8px; font-size: 11px; cursor: pointer; border-bottom: 1px solid #21262d; }
  .sug:hover { background: #21262d; }
  .sug:last-child { border-bottom: none; }
  .sug b { color: #58a6ff; }
  .sug-country { background: rgba(88,166,255,0.06); }
  .sug-country .sug-icon { margin-right: 4px; font-size: 12px; }
  #btn-row { display: flex; gap: 4px; margin: 6px 0 4px; }
  .btn {
    padding: 3px 8px; font-size: 10px; border-radius: 4px; cursor: pointer;
    border: 1px solid #30363d; background: #21262d; color: #c9d1d9;
  }
  .btn:hover { background: #30363d; }

  #tree-wrap {
    padding: 6px 12px 0;
    border-top: 1px solid #30363d; margin-top: 4px;
    max-height: 320px; overflow-y: auto;
  }
  #tree-wrap.expanded { max-height: none; overflow-y: auto; }
  #tree-toggle {
    display: block; width: 100%; padding: 5px 0;
    text-align: center; font-size: 10px; color: #58a6ff;
    cursor: pointer; user-select: none; border-top: 1px solid #21262d;
    background: rgba(22,27,34,0.6);
  }
  #tree-toggle:hover { color: #79c0ff; }
  #tree-wrap::-webkit-scrollbar { width: 4px; }
  #tree-wrap::-webkit-scrollbar-track { background: transparent; }
  #tree-wrap::-webkit-scrollbar-thumb { background: #30363d; border-radius: 2px; }

  .cg { margin-bottom: 1px; }
  .cg-hd {
    display: flex; align-items: center; gap: 4px; padding: 3px 2px;
    cursor: pointer; border-radius: 3px; user-select: none;
  }
  .cg-hd:hover { background: rgba(88,166,255,0.06); }
  .cg-arr {
    display: inline-block; width: 12px; text-align: center;
    font-size: 8px; color: #484f58; transition: transform 0.15s; flex-shrink: 0;
  }
  .cg.open > .cg-hd .cg-arr { transform: rotate(90deg); }
  .cg-hd input[type="checkbox"] {
    accent-color: #1f6feb; cursor: pointer; width: 12px; height: 12px; flex-shrink: 0;
  }
  .cg-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
  .cg-lbl {
    font-size: 11px; font-weight: 600; color: #c9d1d9;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .cg-cnt { font-size: 9px; color: #484f58; margin-left: 1px; flex-shrink: 0; }

  .cg-cities { display: none; padding: 1px 0 4px 0; }
  .cg.open > .cg-cities { display: block; }
  .ct-row {
    display: flex; align-items: center; gap: 4px; padding: 2px 2px 2px 20px;
    cursor: pointer; border-radius: 3px;
  }
  .ct-row:hover { background: rgba(88,166,255,0.06); }
  .ct-row input[type="checkbox"] {
    accent-color: #1f6feb; cursor: pointer; width: 11px; height: 11px; flex-shrink: 0;
  }
  .ct-iata { font-size: 10px; color: #8b949e; font-weight: 600; min-width: 28px; }
  .ct-name {
    font-size: 10px; color: #6e7681;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .ct-row.hl { background: rgba(255,180,0,0.15); }
  .ct-row.hl .ct-iata { color: #ffb800; }
  .ct-row.hl .ct-name { color: #e3b341; }
  @keyframes flash-row { 0%,100% { background: transparent; } 40% { background: rgba(88,166,255,0.25); } }
  .ct-row.flash { animation: flash-row 0.6s ease 2; }
  .node-label {
    display: none; position: absolute; left: calc(100% + 4px); top: 50%;
    transform: translateY(-50%); white-space: nowrap;
    font: 600 10px/1 -apple-system, BlinkMacSystemFont, sans-serif;
    color: #fff; background: rgba(13,17,23,0.85); padding: 2px 5px;
    border-radius: 3px; pointer-events: none;
  }
  .node-label.show { display: block; }

  #filter-box {
    background: rgba(22,27,34,0.94); border: 1px solid #30363d;
    border-radius: 10px; padding: 10px 12px; backdrop-filter: blur(10px);
  }
  #filter-label {
    display: flex; align-items: center; gap: 6px;
    font-size: 11px; color: #c9d1d9; cursor: pointer;
  }
  #filter-label.off { opacity: 0.35; cursor: default; }
  #filter-box input[type="checkbox"] {
    accent-color: #1f6feb; cursor: pointer; width: 13px; height: 13px;
  }
  #filter-desc {
    font-size: 9px; color: #484f58; margin-top: 4px; line-height: 1.3;
  }

  #tf-box {
    background: rgba(22,27,34,0.94); border: 1px solid #30363d;
    border-radius: 10px; padding: 10px 12px; backdrop-filter: blur(10px);
  }
  #tf-box h4 { font-size: 12px; color: #58a6ff; margin-bottom: 8px; }
  #tf-row {
    display: flex; gap: 5px; align-items: flex-end;
  }
  .tf-field { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px; }
  .tf-field label {
    font-size: 9px; font-weight: 600; color: #8b949e;
    text-transform: uppercase; letter-spacing: 0.5px;
  }
  .tf-input-wrap {
    position: relative; cursor: pointer;
  }
  .tf-input-wrap input[type="date"] {
    width: 100%; padding: 7px 8px; font-size: 12px;
    background: #0d1117; border: 1px solid #30363d;
    border-radius: 6px; color: #c9d1d9; outline: none;
    cursor: pointer;
    transition: border-color 0.15s, box-shadow 0.15s;
    -webkit-appearance: none;
  }
  .tf-input-wrap input[type="date"]:focus {
    border-color: #58a6ff;
    box-shadow: 0 0 0 2px rgba(88,166,255,0.15);
  }
  .tf-input-wrap input[type="date"]::-webkit-calendar-picker-indicator {
    position: absolute; inset: 0; width: 100%; height: 100%;
    opacity: 0; cursor: pointer;
  }
  .tf-input-wrap::after {
    content: "\1F4C5"; position: absolute; right: 8px; top: 50%;
    transform: translateY(-50%); font-size: 13px;
    pointer-events: none; opacity: 0.45;
  }
  #tf-search-btn {
    padding: 7px 12px; font-size: 11px; font-weight: 600;
    border-radius: 6px; cursor: pointer; white-space: nowrap;
    border: 1px solid #238636; background: #238636; color: #fff;
    transition: background 0.15s;
    align-self: flex-end;
  }
  #tf-search-btn:hover { background: #2ea043; }
  #tf-clear-btn {
    padding: 7px 8px; font-size: 11px;
    border-radius: 6px; cursor: pointer; white-space: nowrap;
    border: 1px solid #30363d; background: transparent; color: #8b949e;
    transition: color 0.15s;
    align-self: flex-end;
  }
  #tf-clear-btn:hover { color: #c9d1d9; }
  #tf-status { font-size: 10px; color: #8b949e; margin-top: 6px; line-height: 1.4; }

  #pf-box {
    background: rgba(22,27,34,0.94); border: 1px solid #30363d;
    border-radius: 10px; padding: 10px 12px; backdrop-filter: blur(10px);
  }
  #pf-box h4 { font-size: 12px; color: #58a6ff; margin-bottom: 6px; }
  #pf-modes { display: flex; gap: 10px; font-size: 11px; color: #8b949e; margin-bottom: 6px; }
  #pf-modes label { display: flex; align-items: center; gap: 3px; cursor: pointer; }
  #pf-modes input { accent-color: #1f6feb; }
  #pf-controls { display: flex; align-items: center; gap: 5px; margin-bottom: 6px; }
  #pf-controls span { font-size: 11px; color: #8b949e; }
  #pf-n {
    width: 44px; padding: 3px 5px; font-size: 11px;
    background: #161b22; border: 1px solid #30363d; border-radius: 4px;
    color: #c9d1d9; text-align: center; outline: none;
  }
  #pf-n:focus { border-color: #58a6ff; }
  #pf-hop-filter { margin-bottom: 6px; display: none; }
  #pf-hop-filter.active { display: block; }
  #pf-hop-bar {
    display: flex; align-items: flex-start; gap: 0;
    flex-wrap: wrap; margin-bottom: 2px;
  }
  .pf-hop-sep {
    color: #484f58; font-size: 11px; line-height: 24px; padding: 0 2px;
    flex-shrink: 0; user-select: none;
  }
  .pf-hop-slot {
    position: relative; min-width: 48px; flex: 0 1 auto;
  }
  .pf-hop-tags {
    display: flex; flex-wrap: wrap; gap: 2px; align-items: center;
    padding: 2px 3px; min-height: 22px;
    background: #161b22; border: 1px solid #30363d; border-radius: 4px;
    cursor: text;
  }
  .pf-hop-tags:focus-within { border-color: #58a6ff; }
  .pf-hop-tag {
    display: inline-flex; align-items: center; gap: 2px;
    background: rgba(88,166,255,0.15); color: #58a6ff;
    font-size: 9px; padding: 1px 4px; border-radius: 3px;
    white-space: nowrap; max-width: 90px; overflow: hidden;
  }
  .pf-hop-tag-x {
    cursor: pointer; font-size: 10px; color: #8b949e;
    margin-left: 1px; line-height: 1;
  }
  .pf-hop-tag-x:hover { color: #f85149; }
  .pf-hop-inp {
    border: none; background: transparent; color: #c9d1d9;
    font-size: 10px; outline: none; width: 36px; min-width: 20px;
    padding: 1px 0;
  }
  .pf-hop-inp::placeholder { color: #484f58; }
  .pf-hop-suggest {
    position: absolute; top: 100%; left: 0; min-width: 140px; z-index: 20;
    background: #1c2128; border: 1px solid #30363d; border-radius: 4px;
    max-height: 120px; overflow-y: auto; display: none;
  }
  .pf-hop-suggest.show { display: block; }
  .pf-hop-opt {
    padding: 3px 6px; font-size: 10px; color: #c9d1d9; cursor: pointer;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .pf-hop-opt:hover { background: rgba(88,166,255,0.12); }
  #pf-status { font-size: 10px; color: #8b949e; margin-bottom: 4px; }
  #pf-progress {
    display: none; height: 3px; background: #21262d;
    border-radius: 2px; margin-bottom: 6px; overflow: hidden;
  }
  #pf-progress.active { display: block; }
  #pf-progress-bar {
    height: 100%; width: 0%; background: #58a6ff; border-radius: 2px;
    transition: width 0.2s ease;
  }
  #pf-results { overflow-y: visible; }
  #pf-results::-webkit-scrollbar { width: 4px; }
  #pf-results::-webkit-scrollbar-track { background: transparent; }
  #pf-results::-webkit-scrollbar-thumb { background: #30363d; border-radius: 2px; }
  .pf-grp {}
  .pf-grp-hd {
    display: flex; align-items: center; gap: 4px; padding: 3px 6px;
    cursor: pointer; font-size: 11px; color: #58a6ff; user-select: none;
  }
  .pf-grp-hd input[type="checkbox"], .pf-row input[type="checkbox"] {
    accent-color: #e3b341; width: 12px; height: 12px; margin: 0;
    cursor: pointer; flex-shrink: 0;
  }
  .pf-grp-hd:hover { background: rgba(88,166,255,0.06); }
  .pf-grp-arr {
    display: inline-block; font-size: 8px; color: #8b949e;
    transition: transform 0.15s; transform: rotate(0deg);
  }
  .pf-grp.open > .pf-grp-hd .pf-grp-arr { transform: rotate(90deg); }
  .pf-grp-body { display: none; padding: 1px 0 3px 0; }
  .pf-grp.open > .pf-grp-body { display: block; }
  .pf-row {
    display: flex; align-items: center; gap: 4px;
    font-size: 10px; color: #8b949e; padding: 3px 6px 3px 14px; cursor: pointer;
    border-radius: 3px;
  }
  .pf-row-cost {
    font-size: 10px; font-weight: 700; color: #3fb950; min-width: 38px;
    text-align: right; flex-shrink: 0;
  }
  .pf-row-lbl {
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1; min-width: 0;
  }
  .pf-row:hover { background: rgba(255,180,0,0.1); color: #c9d1d9; }
  .pf-row.on { background: rgba(255,180,0,0.18); color: #ffb800; }
  .pf-row.selected { background: rgba(255,180,0,0.22); color: #ffb800; font-weight: 600; }

  #airline-box {
    background: rgba(22,27,34,0.94); border: 1px solid #30363d;
    border-radius: 10px; padding: 10px 12px; backdrop-filter: blur(10px);
  }
  #airline-box h4 { font-size: 12px; color: #58a6ff; margin-bottom: 6px; }
  .al-row {
    display: flex; align-items: center; gap: 6px;
    font-size: 11px; color: #c9d1d9; padding: 3px 0; cursor: pointer;
  }
  .al-row input[type="checkbox"] {
    accent-color: #1f6feb; cursor: pointer; width: 13px; height: 13px;
  }
  .al-dot {
    width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0;
  }
  .al-style {
    font-size: 9px; color: #484f58; margin-left: auto; flex-shrink: 0;
  }

  #arc-popup-overlay {
    display: none; position: fixed; inset: 0; z-index: 5000;
    background: rgba(0,0,0,0.5); align-items: center; justify-content: center;
  }
  #arc-popup-overlay.show { display: flex; }
  #arc-popup {
    background: #161b22; border: 1px solid #30363d; border-radius: 12px;
    width: 420px; max-height: 70vh; display: flex; flex-direction: column;
    box-shadow: 0 8px 32px rgba(0,0,0,0.5);
  }
  #arc-popup-hd {
    padding: 14px 16px 10px; border-bottom: 1px solid #30363d;
    display: flex; align-items: center; gap: 10px; flex-shrink: 0;
  }
  #arc-popup-hd .ap-dot {
    width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
  }
  #arc-popup-hd .ap-route {
    font-size: 15px; font-weight: 700; color: #c9d1d9; flex: 1;
  }
  #arc-popup-hd .ap-airline {
    font-size: 11px; color: #8b949e;
  }
  #arc-popup-close {
    background: none; border: none; color: #8b949e; font-size: 18px;
    cursor: pointer; padding: 0 2px; line-height: 1;
  }
  #arc-popup-close:hover { color: #f85149; }
  #arc-popup-body {
    overflow-y: auto; padding: 4px 0; flex: 1;
  }
  #arc-popup-body::-webkit-scrollbar { width: 5px; }
  #arc-popup-body::-webkit-scrollbar-track { background: transparent; }
  #arc-popup-body::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }
  .ap-row {
    display: flex; align-items: center; gap: 8px;
    padding: 8px 16px; cursor: pointer; border-bottom: 1px solid #21262d;
    transition: background 0.1s;
  }
  .ap-row:hover { background: rgba(88,166,255,0.08); }
  .ap-row:last-child { border-bottom: none; }
  .ap-date { font-size: 13px; color: #c9d1d9; font-weight: 600; min-width: 100px; }
  .ap-day { font-size: 11px; color: #8b949e; min-width: 32px; }
  .ap-price {
    font-size: 13px; font-weight: 700; margin-left: auto; flex-shrink: 0;
  }
  .ap-go {
    font-size: 11px; color: #58a6ff; flex-shrink: 0;
  }
  #arc-popup-empty {
    padding: 20px 16px; text-align: center; color: #484f58; font-size: 12px;
  }
</style>
</head>
<body>
<div id="globeViz"></div>

<div id="left-col">
<div id="left-col-resize"></div>
  <div id="panel">
    <div id="panel-top">
      <h2>Flight Route Network</h2>
      <div class="stat">Airports: <b>__NODE_COUNT__</b> &middot; Routes: <b>__EDGE_COUNT__</b></div>
      <div id="status">Select cities to show routes</div>
      <div id="search-wrap">
        <input id="search" type="text" placeholder="Search airport..." autocomplete="off">
        <div id="suggestions"></div>
      </div>
  <div id="btn-row">
    <button class="btn" onclick="resetView()">Reset view</button>
        <button class="btn" onclick="clearAll()">Clear selection</button>
  </div>
</div>
    <div id="tree-wrap">
      <div id="select-all-row" style="display:flex;align-items:center;gap:5px;padding:4px 2px 6px;border-bottom:1px solid #30363d;margin-bottom:4px;">
        <input type="checkbox" id="select-all-cb" style="accent-color:#1f6feb;cursor:pointer;width:12px;height:12px;">
        <span style="font-size:11px;font-weight:600;color:#8b949e;cursor:pointer;" onclick="document.getElementById('select-all-cb').click()">Select all</span>
      </div>
      <div id="tree"></div>
    </div>
    <div id="tree-toggle" onclick="var tw=document.getElementById('tree-wrap');tw.classList.toggle('expanded');this.textContent=tw.classList.contains('expanded')?'Show less \u25B2':'Show more \u25BC'">Show more &#9660;</div>
  </div>
  <div id="airline-box">
    <h4>Airlines</h4>
    <div id="airline-list"></div>
  </div>
  <div id="filter-box">
    <label id="filter-label" class="off">
      <input type="checkbox" id="intersect-cb" disabled>
      Show only shared routes
    </label>
    <div id="filter-desc">
      When 2+ cities selected, show only routes connecting the selected cities to each other
    </div>
  </div>
  <div id="tf-box">
    <h4>Time Frame</h4>
    <div id="tf-row">
      <div class="tf-field">
        <label for="tf-start">From</label>
        <div class="tf-input-wrap"><input type="date" id="tf-start"></div>
      </div>
      <div class="tf-field">
        <label for="tf-end">To</label>
        <div class="tf-input-wrap"><input type="date" id="tf-end"></div>
      </div>
      <button id="tf-search-btn">Search</button>
      <button id="tf-clear-btn">Clear</button>
    </div>
    <div id="tf-status"></div>
  </div>
  <div id="pf-box">
    <h4>Pathfinder</h4>
    <div id="pf-modes">
      <label><input type="radio" name="pf-mode" value="paths" checked> Paths</label>
      <label><input type="radio" name="pf-mode" value="cycles"> Cycles</label>
    </div>
    <label id="pf-only-label" style="display:flex;align-items:center;gap:4px;font-size:11px;color:#8b949e;margin-bottom:6px;cursor:pointer;">
      <input type="checkbox" id="pf-only-selected" checked style="accent-color:#1f6feb;">
      Only selected cities in path
    </label>
    <div id="pf-controls">
      <span>Length:</span>
      <input type="number" id="pf-n" placeholder="all" min="3" max="8" oninput="if(pfActive)renderPfResults()">
      <button class="btn" onclick="try{runPathfinder()}catch(e){document.getElementById('pf-status').textContent='ERROR: '+e.message;console.error(e);}">Find</button>
      <button class="btn" onclick="clearPathfinder()">Clear</button>
    </div>
    <div id="pf-hop-filter"></div>
    <div id="pf-progress"><div id="pf-progress-bar"></div></div>
    <div id="pf-status"></div>
    <div id="pf-results"></div>
  </div>
</div>

<div id="arc-popup-overlay">
  <div id="arc-popup">
    <div id="arc-popup-hd">
      <span class="ap-dot" id="ap-dot"></span>
      <span class="ap-route" id="ap-route"></span>
      <span class="ap-airline" id="ap-airline"></span>
      <button id="arc-popup-close">&times;</button>
    </div>
    <div id="arc-popup-body"></div>
  </div>
</div>

<script>
var nodesData = __NODES__;
var edgesData = __EDGES__;
var availData = __AVAIL__;
var airlineMeta = __AIRLINE_META__;
var _rawFares = __FARE_DATA__;

var fareCurrencies = _rawFares._c || [];
var fareEurRates = _rawFares._r || [];
var fareByRoute = {};
(function() {
  var BASE = Date.UTC(2026, 0, 1);
  Object.keys(_rawFares).forEach(function(rk) {
    if (rk === "_c" || rk === "_r") return;
    fareByRoute[rk] = _rawFares[rk].map(function(e) {
      var d = new Date(BASE + e[0] * 86400000);
      var ds = d.toISOString().slice(0, 10);
      var cur = fareCurrencies[e[2]];
      var rate = fareEurRates[e[2]] || 1;
      var eur = e[1] / rate;
      return {date: ds, price: e[1], currency: cur, airline: e[3], eur: Math.round(eur * 100) / 100};
    });
  });
  _rawFares = null;
})();
var intersectMode = false;
var tfEnabled = false;
var tfActiveEdges = null;

var nodeMap = {};
nodesData.forEach(function(n) { nodeMap[n.id] = n; });

var activeCities = new Set();
var activeAirlines = new Set(Object.keys(airlineMeta));

var nodeAirlines = {};
var edgeAirlines = {};
edgesData.forEach(function(e) {
  (e.airlines || []).forEach(function(al) {
    if (!nodeAirlines[e.from]) nodeAirlines[e.from] = new Set();
    if (!nodeAirlines[e.to]) nodeAirlines[e.to] = new Set();
    nodeAirlines[e.from].add(al);
    nodeAirlines[e.to].add(al);
  });
  edgeAirlines[e.from + "-" + e.to] = e.airlines || [];
  edgeAirlines[e.to + "-" + e.from] = e.airlines || [];
});

function edgeAirlineColor(from, to) {
  var als = edgeAirlines[from + "-" + to] || [];
  for (var i = 0; i < als.length; i++) { if (activeAirlines.has(als[i])) return airlineArcColor(als[i]); }
  return "#888888";
}

function edgeAirlineCode(from, to) {
  var als = edgeAirlines[from + "-" + to] || [];
  for (var i = 0; i < als.length; i++) { if (activeAirlines.has(als[i])) return als[i]; }
  return als[0] || "FR";
}

function isNodeVisible(id) {
  var als = nodeAirlines[id];
  if (!als) return false;
  var it = als.values(), v;
  while (!(v = it.next()).done) { if (activeAirlines.has(v.value)) return true; }
  return false;
}

function isEdgeVisible(e) {
  var als = e.airlines || [];
  for (var i = 0; i < als.length; i++) { if (activeAirlines.has(als[i])) return true; }
  return false;
}

function edgeAirline(e) {
  var als = e.airlines || [];
  for (var i = 0; i < als.length; i++) { if (activeAirlines.has(als[i])) return als[i]; }
  return als[0] || "FR";
}

var outDeg = {}, inDeg = {};
edgesData.forEach(function(e) {
  outDeg[e.from] = (outDeg[e.from] || 0) + 1;
  inDeg[e.to] = (inDeg[e.to] || 0) + 1;
});

var countryGroups = {}, countryColors = {}, countryNames = {};
nodesData.forEach(function(n) {
  var cc = (n.country || "").toLowerCase();
  if (!cc) return;
  if (!countryGroups[cc]) {
    countryGroups[cc] = [];
    countryColors[cc] = n.color;
    countryNames[cc] = n.country_name || cc.toUpperCase();
  }
  countryGroups[cc].push(n);
});
Object.keys(countryGroups).forEach(function(cc) {
  countryGroups[cc].sort(function(a, b) { return a.id.localeCompare(b.id); });
});

function hexToRgba(hex, a) {
  var r = parseInt(hex.slice(1,3), 16);
  var g = parseInt(hex.slice(3,5), 16);
  var b = parseInt(hex.slice(5,7), 16);
  return "rgba(" + r + "," + g + "," + b + "," + a + ")";
}

/* ---- Globe: flat HTML circle nodes ---- */
var nodeEls = {};
var cityRows = {};
var DEG2RAD = Math.PI / 180;
function calcBearing(lat1, lng1, lat2, lng2) {
  var la1 = lat1 * DEG2RAD, la2 = lat2 * DEG2RAD;
  var dLng = (lng2 - lng1) * DEG2RAD;
  var y = Math.sin(dLng) * Math.cos(la2);
  var x = Math.cos(la1) * Math.sin(la2) - Math.sin(la1) * Math.cos(la2) * Math.cos(dLng);
  return Math.atan2(y, x) / DEG2RAD;
}

function destPoint(lat, lng, bearing, distKm) {
  var R = 6371;
  var d = distKm / R;
  var brng = bearing * DEG2RAD;
  var la1 = lat * DEG2RAD;
  var lo1 = lng * DEG2RAD;
  var la2 = Math.asin(Math.sin(la1) * Math.cos(d) + Math.cos(la1) * Math.sin(d) * Math.cos(brng));
  var lo2 = lo1 + Math.atan2(Math.sin(brng) * Math.sin(d) * Math.cos(la1), Math.cos(d) - Math.sin(la1) * Math.sin(la2));
  return {lat: la2 / DEG2RAD, lng: lo2 / DEG2RAD};
}

function addArrowArcs(arcs, fromLat, fromLng, toLat, toLng, color, alt, fromIata, toIata, airline) {
  var brng = calcBearing(fromLat, fromLng, toLat, toLng);
  var rev = brng + 180;
  var wingLen = 22;
  var spread = 24;
  var lp = destPoint(toLat, toLng, rev + spread, wingLen);
  var rp = destPoint(toLat, toLng, rev - spread, wingLen);
  arcs.push({
    startLat: lp.lat, startLng: lp.lng,
    endLat: toLat, endLng: toLng,
    color: color, alt: alt,
    fromIata: fromIata, toIata: toIata, airline: airline
  });
  arcs.push({
    startLat: rp.lat, startLng: rp.lng,
    endLat: toLat, endLng: toLng,
    color: color, alt: alt,
    fromIata: fromIata, toIata: toIata, airline: airline
  });
}

function createNodeEl(d) {
  var sz = Math.max(4, 2.5 + d.size * 0.12);
  var el = document.createElement("div");
  el.style.cssText = "width:" + sz + "px;height:" + sz + "px;border-radius:50%;" +
    "background:" + d.color + ";pointer-events:auto;cursor:pointer;" +
    "border:0.5px solid rgba(255,255,255,0.25);transition:box-shadow 0.2s;" +
    "position:relative;";
  el.title = d.name + " (" + d.id + ") - " + d.city + ", " +
    (d.country_name || d.country.toUpperCase());
  var lbl = document.createElement("span");
  lbl.className = "node-label";
  lbl.textContent = (d.city || d.name) + " (" + d.id + ")";
  el.appendChild(lbl);
  el.addEventListener("click", function(ev) {
    ev.stopPropagation();
    handlePointClick(d);
  });
  el.addEventListener("mouseenter", function() {
    lbl.classList.add("show");
    if (cityRows[d.id]) cityRows[d.id].classList.add("hl");
  });
  el.addEventListener("mouseleave", function() {
    lbl.classList.remove("show");
    if (cityRows[d.id]) cityRows[d.id].classList.remove("hl");
  });
  nodeEls[d.id] = el;
  return el;
}

var myGlobe = Globe()
  .globeImageUrl("https://unpkg.com/three-globe/example/img/earth-night.jpg")
  .backgroundImageUrl("https://unpkg.com/three-globe/example/img/night-sky.png")
  .showAtmosphere(true)
  .atmosphereColor("#1a3366")
  .atmosphereAltitude(0.15)
  .htmlElementsData(nodesData)
  .htmlLat("lat")
  .htmlLng("lon")
  .htmlAltitude(0.002)
  .htmlElement(createNodeEl)
  .arcsData([])
  .arcStartLat("startLat")
  .arcStartLng("startLng")
  .arcEndLat("endLat")
  .arcEndLng("endLng")
  .arcColor("color")
  .arcStroke(0.2)
  .arcDashLength(1)
  .arcDashGap(0)
  .arcAltitude(function(d) { return d.alt != null ? d.alt : undefined; })
  .arcAltitudeAutoScale(0.3)
  .width(window.innerWidth)
  .height(window.innerHeight)
  (document.getElementById("globeViz"));

function nextAvailDate(fromIata, toIata) {
  var today = new Date().toISOString().slice(0, 10);
  var dates = availData[fromIata + "-" + toIata] || [];
  for (var i = 0; i < dates.length; i++) {
    if (dates[i] >= today) return dates[i];
  }
  var d = new Date();
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}

function buildAirlineUrl(airline, fromIata, toIata, dateStr) {
  if (!dateStr) dateStr = nextAvailDate(fromIata, toIata);
  if (airline === "W6") {
    return "https://wizzair.com/en-gb/booking/select-flight/" +
      fromIata + "/" + toIata + "/" + dateStr + "/null/1/0/0/0/null";
  }
  return "https://www.ryanair.com/gb/en/trip/flights/select" +
    "?adults=1&teens=0&children=0&infants=0" +
    "&dateOut=" + dateStr +
    "&isConnectedFlight=false&isReturn=false" +
    "&originIata=" + fromIata +
    "&destinationIata=" + toIata;
}

function showArcPopup(arc) {
  if (!arc || !arc.fromIata || !arc.toIata) return;
  var from = arc.fromIata, to = arc.toIata;
  var al = arc.airline || edgeAirlineCode(from, to);
  var meta = airlineMeta[al] || {name: al, color: "#888"};
  var fNode = nodeMap[from], tNode = nodeMap[to];
  var fromLabel = fNode ? (fNode.city || fNode.name) + " (" + from + ")" : from;
  var toLabel = tNode ? (tNode.city || tNode.name) + " (" + to + ")" : to;

  document.getElementById("ap-dot").style.background = meta.color;
  document.getElementById("ap-route").textContent = fromLabel + "  \u2192  " + toLabel;
  document.getElementById("ap-airline").textContent = meta.name;

  var flights = fareByRoute[from + "-" + to] || [];
  var today = new Date().toISOString().slice(0, 10);
  flights = flights.filter(function(f) { return f.date >= today && f.price > 0; });

  if (tfEnabled && tfActiveEdges) {
    var start = document.getElementById("tf-start").value;
    var end = document.getElementById("tf-end").value;
    if (start && end) {
      flights = flights.filter(function(f) { return f.date >= start && f.date <= end; });
    }
  }

  var body = document.getElementById("arc-popup-body");
  body.innerHTML = "";

  if (flights.length === 0) {
    var dates = availData[from + "-" + to] || [];
    dates = dates.filter(function(d) { return d >= today; });
    if (tfEnabled) {
      var start = document.getElementById("tf-start").value;
      var end = document.getElementById("tf-end").value;
      if (start && end) dates = dates.filter(function(d) { return d >= start && d <= end; });
    }
    if (dates.length > 0) {
      dates.forEach(function(dt) {
        var row = document.createElement("div");
        row.className = "ap-row";
        var dd = new Date(dt + "T00:00:00");
        var dayName = dd.toLocaleDateString("en-GB", {weekday: "short"});
        var dateLabel = dd.toLocaleDateString("en-GB", {day: "numeric", month: "short", year: "numeric"});
        row.innerHTML = '<span class="ap-day">' + dayName + '</span>' +
          '<span class="ap-date">' + dateLabel + '</span>' +
          '<span class="ap-price" style="color:#8b949e">--</span>' +
          '<span class="ap-go">\u2192 Book</span>';
        row.addEventListener("click", function() {
          window.open(buildAirlineUrl(al, from, to, dt), "_blank");
        });
        body.appendChild(row);
      });
    } else {
      body.innerHTML = '<div id="arc-popup-empty">No flights found in this time frame</div>';
    }
  } else {
    flights.forEach(function(f) {
      var row = document.createElement("div");
      row.className = "ap-row";
      var dd = new Date(f.date + "T00:00:00");
      var dayName = dd.toLocaleDateString("en-GB", {weekday: "short"});
      var dateLabel = dd.toLocaleDateString("en-GB", {day: "numeric", month: "short", year: "numeric"});
      var priceColor = meta.color;
      var priceStr;
      if (f.currency === "EUR") {
        priceStr = f.price.toFixed(2) + " \u20AC";
      } else {
        priceStr = f.eur.toFixed(2) + " \u20AC" +
          ' <span style="color:#8b949e;font-size:10px">(' +
          Math.round(f.price) + " " + f.currency + ")</span>";
      }
      row.innerHTML = '<span class="ap-day">' + dayName + '</span>' +
        '<span class="ap-date">' + dateLabel + '</span>' +
        '<span class="ap-price" style="color:' + priceColor + '">' + priceStr + '</span>' +
        '<span class="ap-go">\u2192 Book</span>';
      row.addEventListener("click", function() {
        window.open(buildAirlineUrl(al, from, to, f.date), "_blank");
      });
      body.appendChild(row);
    });
  }

  document.getElementById("arc-popup-overlay").classList.add("show");
}

document.getElementById("arc-popup-close").addEventListener("click", function() {
  document.getElementById("arc-popup-overlay").classList.remove("show");
});
document.getElementById("arc-popup-overlay").addEventListener("click", function(e) {
  if (e.target === this) this.classList.remove("show");
});
document.addEventListener("keydown", function(e) {
  if (e.key === "Escape") document.getElementById("arc-popup-overlay").classList.remove("show");
});

myGlobe.onArcClick(function(arc) {
  showArcPopup(arc);
});

myGlobe.onArcHover(function(arc) {
  document.body.style.cursor = arc && arc.fromIata ? "pointer" : "";
});

fetch("https://unpkg.com/world-atlas@2/countries-110m.json")
  .then(function(r) { return r.json(); })
  .then(function(world) {
    var countries = topojson.feature(world, world.objects.countries);
    myGlobe
      .polygonsData(countries.features)
      .polygonCapColor(function() { return "rgba(15,40,80,0.45)"; })
      .polygonSideColor(function() { return "rgba(10,25,50,0.15)"; })
      .polygonStrokeColor(function() { return "rgba(180,210,255,0.5)"; })
      .polygonAltitude(0.001);
  });

myGlobe.pointOfView({ lat: 50, lng: 10, altitude: 2.5 }, 0);

var ctrl = myGlobe.controls();
ctrl.autoRotate = true;
ctrl.autoRotateSpeed = 0.35;
ctrl.addEventListener("start", function() { ctrl.autoRotate = false; });

var baseArcStroke = 0.2;
var REF_DIST = 550;
var _lastStroke = 0;
var _strokeTimer = null;
function getZoomStroke(base) {
  var d = myGlobe.camera().position.length();
  return base * (Math.max(d, 50) / REF_DIST);
}
function applyZoomStroke() {
  var s = getZoomStroke(baseArcStroke);
  if (Math.abs(s - _lastStroke) / (s || 1) < 0.05) return;
  _lastStroke = s;
  myGlobe.arcStroke(s);
}
ctrl.addEventListener("change", function() {
  if (_strokeTimer) return;
  _strokeTimer = setTimeout(function() { _strokeTimer = null; applyZoomStroke(); }, 150);
});

window.addEventListener("resize", function() {
  myGlobe.width(window.innerWidth).height(window.innerHeight);
});

/* ---- Selection ---- */
function handlePointClick(point) {
  if (!point) return;
  toggleCity(point.id);
}

function toggleCity(id) {
  if (activeCities.has(id)) activeCities.delete(id);
  else activeCities.add(id);
  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
}

function toggleCountry(cc, checked) {
  (countryGroups[cc] || []).forEach(function(c) {
    if (checked) activeCities.add(c.id);
    else activeCities.delete(c.id);
  });
  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
}

var ARC_ALT_BASE = 0.003;
var ARC_ALT_OFFSET = 0.012;

function airlineArcColor(code) {
  var m = airlineMeta[code];
  return m ? m.color : "#888888";
}

function refreshArcs() {
  if (pfActive) return;
  if (activeCities.size === 0) {
    myGlobe.arcsData([]);
    return;
  }
  var arcs = [];
  edgesData.forEach(function(e) {
    if (!isEdgeVisible(e)) return;
    if (tfEnabled && tfActiveEdges && !tfActiveEdges.has(e.from + "-" + e.to)) return;
    var show;
    if (intersectMode && activeCities.size >= 2) {
      show = activeCities.has(e.from) && activeCities.has(e.to);
    } else {
      show = activeCities.has(e.from) || activeCities.has(e.to);
    }
    if (!show) return;
    var f = nodeMap[e.from], t = nodeMap[e.to];
    if (!f || !t) return;

    var visAirlines = (e.airlines || []).filter(function(a) { return activeAirlines.has(a); });
    var multi = visAirlines.length > 1;

    for (var ai = 0; ai < visAirlines.length; ai++) {
      var al = visAirlines[ai];
      var c = airlineArcColor(al);
      var alt = multi ? ARC_ALT_BASE + ai * ARC_ALT_OFFSET : undefined;
      arcs.push({
        startLat: f.lat, startLng: f.lon,
        endLat: t.lat, endLng: t.lon,
        color: c, alt: alt,
        fromIata: e.from, toIata: e.to, airline: al
      });
      addArrowArcs(arcs, f.lat, f.lon, t.lat, t.lon, c, alt, e.from, e.to, al);
    }
  });
  myGlobe.arcsData(arcs);
}

/* ---- Build tree ---- */
var treeEl = document.getElementById("tree");

Object.keys(countryGroups).sort(function(a, b) {
  return (countryNames[a] || a).localeCompare(countryNames[b] || b);
}).forEach(function(cc) {
  var cities = countryGroups[cc];

  var g = document.createElement("div");
  g.className = "cg";
  g.dataset.cc = cc;

  var hd = document.createElement("div");
  hd.className = "cg-hd";

  var arrow = document.createElement("span");
  arrow.className = "cg-arr";
  arrow.innerHTML = "&#9654;";

  var cb = document.createElement("input");
  cb.type = "checkbox";
  cb.dataset.cc = cc;

  var dot = document.createElement("span");
  dot.className = "cg-dot";
  dot.style.background = countryColors[cc];

  var lbl = document.createElement("span");
  lbl.className = "cg-lbl";
  lbl.textContent = countryNames[cc] || cc.toUpperCase();

  var cnt = document.createElement("span");
  cnt.className = "cg-cnt";
  cnt.textContent = "(" + cities.length + ")";

  hd.appendChild(arrow);
  hd.appendChild(cb);
  hd.appendChild(dot);
  hd.appendChild(lbl);
  hd.appendChild(cnt);

  var cityDiv = document.createElement("div");
  cityDiv.className = "cg-cities";

  cities.forEach(function(c) {
    var row = document.createElement("div");
    row.className = "ct-row";

    var ccb = document.createElement("input");
    ccb.type = "checkbox";
    ccb.dataset.id = c.id;

    var iata = document.createElement("span");
    iata.className = "ct-iata";
    iata.textContent = c.id;

    var nm = document.createElement("span");
    nm.className = "ct-name";
    nm.textContent = c.city || c.name;
    nm.title = c.name + " - " + (outDeg[c.id] || 0) + " out / " + (inDeg[c.id] || 0) + " in";

    row.appendChild(ccb);
    row.appendChild(iata);
    row.appendChild(nm);

    ccb.addEventListener("click", function(ev) { ev.stopPropagation(); toggleCity(c.id); });
    row.addEventListener("click", function(ev) {
      if (ev.target.type === "checkbox") return;
      toggleCity(c.id);
    });
    row.addEventListener("mouseenter", function() {
      var el = nodeEls[c.id];
      if (el) el.querySelector(".node-label").classList.add("show");
    });
    row.addEventListener("mouseleave", function() {
      var el = nodeEls[c.id];
      if (el) el.querySelector(".node-label").classList.remove("show");
    });
    cityRows[c.id] = row;

    cityDiv.appendChild(row);
  });

  cb.addEventListener("click", function(ev) {
    ev.stopPropagation();
    toggleCountry(cc, this.checked);
    if (this.checked && !g.classList.contains("open")) g.classList.add("open");
  });

  hd.addEventListener("click", function(ev) {
    if (ev.target.type === "checkbox") return;
    g.classList.toggle("open");
  });

  g.appendChild(hd);
  g.appendChild(cityDiv);
  treeEl.appendChild(g);
});

/* ---- Airline panel ---- */
(function() {
  var listEl = document.getElementById("airline-list");
  var codes = Object.keys(airlineMeta).sort();
  codes.forEach(function(code) {
    var meta = airlineMeta[code];
    var row = document.createElement("label");
    row.className = "al-row";

    var cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = true;
    cb.dataset.al = code;

    var dot = document.createElement("span");
    dot.className = "al-dot";
    dot.style.background = meta.color;

    var name = document.createElement("span");
    name.textContent = meta.name + " (" + code + ")";

    var info = document.createElement("span");
    info.className = "al-style";
    info.textContent = meta.routes + " routes";

    row.appendChild(cb);
    row.appendChild(dot);
    row.appendChild(name);
    row.appendChild(info);
    listEl.appendChild(row);

    cb.addEventListener("change", function() {
      if (this.checked) activeAirlines.add(code);
      else activeAirlines.delete(code);
      applyAirlineFilter();
    });
  });
})();

function applyAirlineFilter() {
  // Update node visibility on globe
  var visibleNodes = nodesData.filter(function(n) { return isNodeVisible(n.id); });
  myGlobe.htmlElementsData(visibleNodes);

  // Update tree: show/hide city rows and country groups
  document.querySelectorAll(".ct-row").forEach(function(row) {
    var cb = row.querySelector("input[type='checkbox']");
    var id = cb ? cb.dataset.id : null;
    if (id) {
      row.style.display = isNodeVisible(id) ? "" : "none";
      if (!isNodeVisible(id) && activeCities.has(id)) {
        activeCities.delete(id);
      }
    }
  });

  document.querySelectorAll(".cg").forEach(function(g) {
    var cc = g.dataset.cc;
    var cities = countryGroups[cc] || [];
    var visCount = 0;
    cities.forEach(function(c) { if (isNodeVisible(c.id)) visCount++; });
    g.style.display = visCount > 0 ? "" : "none";
    var cntEl = g.querySelector(".cg-cnt");
    if (cntEl) cntEl.textContent = "(" + visCount + ")";
  });

  // Update stats line
  var visEdges = edgesData.filter(function(e) { return isEdgeVisible(e); });
  document.querySelector(".stat").innerHTML =
    "Airports: <b>" + visibleNodes.length + "</b> &middot; Routes: <b>" + visEdges.length + "</b>";

  adjOut = buildAdjOut();

  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
}

/* ---- Intersection toggle ---- */
document.getElementById("intersect-cb").addEventListener("change", function() {
  intersectMode = this.checked;
  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
});

/* ---- Time frame filter ---- */
function tfWeekLater(dateStr) {
  var d = new Date(dateStr + "T00:00:00");
  d.setDate(d.getDate() + 7);
  return d.toISOString().slice(0, 10);
}

(function() {
  var today = new Date().toISOString().slice(0, 10);
  document.getElementById("tf-start").value = today;
  document.getElementById("tf-end").value = tfWeekLater(today);

  var dates = Object.values(availData).flat();
  if (dates.length) {
    dates.sort();
    document.getElementById("tf-status").textContent =
      "Data available: " + dates[0] + " to " + dates[dates.length - 1] +
      " (" + Object.keys(availData).length + " routes)";
  } else {
    document.getElementById("tf-status").textContent = "No availability data.";
  }
})();

document.getElementById("tf-start").addEventListener("change", function() {
  var endEl = document.getElementById("tf-end");
  var newEnd = tfWeekLater(this.value);
  if (!endEl.value || endEl.value < this.value) {
    endEl.value = newEnd;
  }
});

document.querySelectorAll(".tf-input-wrap").forEach(function(wrap) {
  wrap.addEventListener("click", function() {
    var inp = this.querySelector("input");
    if (inp && inp.showPicker) { try { inp.showPicker(); } catch(e) {} }
  });
});

function applyTimeFrame() {
  var start = document.getElementById("tf-start").value;
  var end = document.getElementById("tf-end").value;
  if (!start || !end) {
    document.getElementById("tf-status").textContent = "Set both start and end dates";
    document.getElementById("tf-status").style.color = "#f85149";
    return;
  }
  if (start > end) {
    document.getElementById("tf-status").textContent = "Start date must be before end date";
    document.getElementById("tf-status").style.color = "#f85149";
    return;
  }
  tfEnabled = true;
  var activeRoutes = new Set();
  Object.keys(availData).forEach(function(routeKey) {
    var days = availData[routeKey];
    for (var i = 0; i < days.length; i++) {
      if (days[i] >= start && days[i] <= end) {
        activeRoutes.add(routeKey);
        break;
      }
    }
  });
  tfActiveEdges = activeRoutes;
  document.getElementById("tf-status").textContent =
    activeRoutes.size + " routes with flights between " + start + " and " + end;
  document.getElementById("tf-status").style.color = "#3fb950";
  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
}

function clearTimeFrame() {
  tfEnabled = false;
  tfActiveEdges = null;
  var today = new Date().toISOString().slice(0, 10);
  document.getElementById("tf-start").value = today;
  document.getElementById("tf-end").value = tfWeekLater(today);
  var dates = Object.values(availData).flat();
  if (dates.length) {
    dates.sort();
    document.getElementById("tf-status").textContent =
      "Data available: " + dates[0] + " to " + dates[dates.length - 1] +
      " (" + Object.keys(availData).length + " routes)";
  } else {
    document.getElementById("tf-status").textContent = "";
  }
  document.getElementById("tf-status").style.color = "#8b949e";
  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
}

document.getElementById("tf-search-btn").addEventListener("click", applyTimeFrame);
document.getElementById("tf-clear-btn").addEventListener("click", clearTimeFrame);

/* ---- Sync UI ---- */
function reorderCountries() {
  var tree = document.getElementById("tree");
  var groups = Array.from(tree.querySelectorAll(".cg"));
  groups.sort(function(a, b) {
    var ccA = a.dataset.cc, ccB = b.dataset.cc;
    var citiesA = countryGroups[ccA] || [], citiesB = countryGroups[ccB] || [];
    var selA = 0, selB = 0;
    citiesA.forEach(function(c) { if (activeCities.has(c.id)) selA++; });
    citiesB.forEach(function(c) { if (activeCities.has(c.id)) selB++; });
    var hasA = selA > 0 ? 0 : 1;
    var hasB = selB > 0 ? 0 : 1;
    if (hasA !== hasB) return hasA - hasB;
    var nameA = (countryNames[ccA] || ccA).toLowerCase();
    var nameB = (countryNames[ccB] || ccB).toLowerCase();
    return nameA < nameB ? -1 : nameA > nameB ? 1 : 0;
  });
  groups.forEach(function(g) {
    tree.appendChild(g);
    var cc = g.dataset.cc;
    var cities = countryGroups[cc] || [];
    var hasSel = cities.some(function(c) { return activeCities.has(c.id); });
    if (hasSel) g.classList.add("open");
    else g.classList.remove("open");
  });
}

function syncUI() {
  document.querySelectorAll(".ct-row input[type='checkbox']").forEach(function(cb) {
    cb.checked = activeCities.has(cb.dataset.id);
  });

  var totalVisible = 0, totalSelected = 0;
  document.querySelectorAll(".cg-hd input[type='checkbox']").forEach(function(cb) {
    var cc = cb.dataset.cc;
    var cities = countryGroups[cc] || [];
    var vis = 0, sel = 0;
    cities.forEach(function(c) {
      if (isNodeVisible(c.id)) { vis++; if (activeCities.has(c.id)) sel++; }
    });
    totalVisible += vis;
    totalSelected += sel;
    cb.checked = sel === vis && vis > 0;
    cb.indeterminate = sel > 0 && sel < vis;
  });
  var sacb = document.getElementById("select-all-cb");
  sacb.checked = totalSelected === totalVisible && totalVisible > 0;
  sacb.indeterminate = totalSelected > 0 && totalSelected < totalVisible;

  reorderCountries();

  Object.keys(nodeEls).forEach(function(id) {
    nodeEls[id].style.boxShadow = activeCities.has(id)
      ? "0 0 5px 2px " + nodeMap[id].color : "none";
  });

  var icb = document.getElementById("intersect-cb");
  var ilbl = document.getElementById("filter-label");
  if (activeCities.size >= 2) {
    icb.disabled = false;
    ilbl.className = "";
  } else {
    icb.disabled = true;
    ilbl.className = "off";
  }

  var s = document.getElementById("status");
  if (activeCities.size > 0) {
    var arcN = myGlobe.arcsData().length;
    s.textContent = activeCities.size + " cities, " + arcN + " routes" +
      (intersectMode && activeCities.size >= 2 ? " (shared only)" : "");
  } else {
    s.textContent = "Select cities to show routes";
  }
}

/* ---- Pathfinder ---- */
function buildAdjOut() {
  var adj = {};
  edgesData.forEach(function(e) {
    if (!isEdgeVisible(e)) return;
    if (!adj[e.from]) adj[e.from] = [];
    adj[e.from].push(e.to);
    if (!adj[e.to]) adj[e.to] = [];
    adj[e.to].push(e.from);
  });
  return adj;
}
var adjOut = buildAdjOut();

var pfActive = false;
var pfResults = [];
var pfHitLimit = false;
var pfHighlight = -1;
var pfSelectedPaths = [];

var _pfRunning = false;

function runPathfinder() {
  if (_pfRunning) return;
  var isCycle = document.querySelector('input[name="pf-mode"]:checked').value === "cycles";
  var onlySelected = document.getElementById("pf-only-selected").checked;
  var nList = isCycle ? [3, 4, 5, 6, 7, 8] : [1, 2, 3, 4, 5, 6, 7, 8];
  var statusEl = document.getElementById("pf-status");
  var resultsEl = document.getElementById("pf-results");
  var progressEl = document.getElementById("pf-progress");
  var progressBar = document.getElementById("pf-progress-bar");
  var label = isCycle ? "cycle" : "path";

  var pfAdj = {};
  edgesData.forEach(function(e) {
    if (!isEdgeVisible(e)) return;
    if (tfEnabled && tfActiveEdges) {
      if (tfActiveEdges.has(e.from + "-" + e.to)) {
        if (!pfAdj[e.from]) pfAdj[e.from] = [];
        pfAdj[e.from].push(e.to);
      }
      if (tfActiveEdges.has(e.to + "-" + e.from)) {
        if (!pfAdj[e.to]) pfAdj[e.to] = [];
        pfAdj[e.to].push(e.from);
      }
    } else {
      if (!pfAdj[e.from]) pfAdj[e.from] = [];
      pfAdj[e.from].push(e.to);
      if (!pfAdj[e.to]) pfAdj[e.to] = [];
      pfAdj[e.to].push(e.from);
    }
  });

  if (activeCities.size < (isCycle ? 1 : 2)) {
    statusEl.textContent = isCycle ? "Select at least 1 city first" : "Select at least 2 cities first";
    resultsEl.innerHTML = "";
    return;
  }

  Object.keys(pfAdj).forEach(function(k) {
    var u = {}; var deduped = [];
    pfAdj[k].forEach(function(v) { if (!u[v]) { u[v] = true; deduped.push(v); } });
    pfAdj[k] = deduped;
  });

  var selected = Array.from(activeCities);
  var selectedSet = activeCities;
  var results = [];
  var seen = {};

  _pfRunning = true;
  progressEl.classList.add("active");
  progressBar.style.width = "0%";
  statusEl.textContent = "Searching ...";
  resultsEl.innerHTML = "";

  var totalSteps = nList.length * selected.length;
  var stepsDone = 0;

  function updateProgress() {
    var pct = Math.min(100, Math.round(stepsDone / totalSteps * 100));
    progressBar.style.width = pct + "%";
    statusEl.textContent = "Searching ... " + results.length + " " + label + (results.length !== 1 ? "s" : "") + " found";
  }

  function dfsFromStart(start, n) {
    var _count = 0;
    function dfs(node, path, vis) {
      if (++_count > 50000) return;
      var hops = path.length - 1;

      if (hops === n) {
        if (!isCycle && selectedSet.has(node) && node !== start) {
          var key = path.join(">");
          if (!seen[key]) { seen[key] = true; results.push(path.slice()); }
        }
        return;
      }

      var nb = pfAdj[node] || [];
      for (var i = 0; i < nb.length; i++) {
        var next = nb[i];
        if (onlySelected && !selectedSet.has(next)) continue;
        if (next === start && isCycle) {
          if (hops === n - 1) {
            path.push(next);
            var key = path.slice().sort().join(">");
            if (!seen[key]) { seen[key] = true; results.push(path.slice()); }
            path.pop();
          }
          continue;
        }
        if (vis[next]) continue;
        vis[next] = true;
        path.push(next);
        dfs(next, path, vis);
        path.pop();
        vis[next] = false;
      }
    }
    var vis = {};
    vis[start] = true;
    dfs(start, [start], vis);
  }

  var li = 0, si = 0;

  function processBatch() {
    var batchStart = Date.now();
    while (li < nList.length) {
      while (si < selected.length) {
        dfsFromStart(selected[si], nList[li]);
        si++;
        stepsDone++;
        if (Date.now() - batchStart > 80) {
          updateProgress();
          setTimeout(processBatch, 0);
          return;
        }
      }
      si = 0;
      li++;
    }
    finishSearch();
  }

  function finishSearch() {
    _pfRunning = false;
    progressBar.style.width = "100%";
    setTimeout(function() { progressEl.classList.remove("active"); }, 400);
    pfResults = results;
    pfHitLimit = false;
    pfActive = true;
    pfHighlight = -1;
    renderPfResults();
  }

  setTimeout(processBatch, 0);
}

function refreshSelectedPfArcs() {
  if (pfSelectedPaths.length > 0) {
    showPfArcs(pfSelectedPaths);
  } else {
    myGlobe.arcsData([]);
  }
}

function togglePfPath(path, row) {
  var key = path.join(">");
  var idx = -1;
  for (var i = 0; i < pfSelectedPaths.length; i++) {
    if (pfSelectedPaths[i].join(">") === key) { idx = i; break; }
  }
  if (idx >= 0) {
    pfSelectedPaths.splice(idx, 1);
    row.classList.remove("selected");
  } else {
    pfSelectedPaths.push(path);
    row.classList.add("selected");
  }
  refreshSelectedPfArcs();
}

var pfHopFilters = [];

function cityName(id) {
  var nd = nodeMap[id];
  return nd ? (nd.city || nd.name) : id;
}

function buildHopFilter() {
  var box = document.getElementById("pf-hop-filter");
  box.innerHTML = "";
  pfHopFilters = [];
  if (pfResults.length === 0) { box.classList.remove("active"); return; }

  var maxLen = 0;
  pfResults.forEach(function(p) { if (p.length > maxLen) maxLen = p.length; });

  box.classList.add("active");
  var bar = document.createElement("div");
  bar.id = "pf-hop-bar";
  box.appendChild(bar);

  for (var i = 0; i < maxLen; i++) {
    if (i > 0) {
      var sep = document.createElement("span");
      sep.className = "pf-hop-sep";
      sep.textContent = ">";
      bar.appendChild(sep);
    }
    (function(pos) {
      var slot = document.createElement("div");
      slot.className = "pf-hop-slot";

      var tagsBox = document.createElement("div");
      tagsBox.className = "pf-hop-tags";

      var inp = document.createElement("input");
      inp.type = "text";
      inp.className = "pf-hop-inp";
      inp.placeholder = "any";

      var suggest = document.createElement("div");
      suggest.className = "pf-hop-suggest";

      tagsBox.appendChild(inp);
      slot.appendChild(tagsBox);
      slot.appendChild(suggest);
      bar.appendChild(slot);

      var hopData = {pos: pos, ids: [], tagsBox: tagsBox, input: inp, suggest: suggest};
      pfHopFilters.push(hopData);

      tagsBox.addEventListener("click", function() { inp.focus(); });

      function addTag(id) {
        if (hopData.ids.indexOf(id) >= 0) return;
        hopData.ids.push(id);
        var tag = document.createElement("span");
        tag.className = "pf-hop-tag";
        tag.textContent = cityName(id);
        tag.title = id;
        var x = document.createElement("span");
        x.className = "pf-hop-tag-x";
        x.textContent = "x";
        x.addEventListener("click", function(ev) {
          ev.stopPropagation();
          var idx = hopData.ids.indexOf(id);
          if (idx >= 0) hopData.ids.splice(idx, 1);
          tag.remove();
          renderPfResultsList();
        });
        tag.appendChild(x);
        tagsBox.insertBefore(tag, inp);
        inp.value = "";
        renderPfResultsList();
      }

      inp.addEventListener("input", function() {
        showHopSuggestions(hopData, inp.value.trim().toLowerCase(), suggest);
      });
      inp.addEventListener("focus", function() {
        showHopSuggestions(hopData, inp.value.trim().toLowerCase(), suggest);
      });
      inp.addEventListener("blur", function() {
        setTimeout(function() { suggest.classList.remove("show"); }, 150);
      });
      inp.addEventListener("keydown", function(ev) {
        if (ev.key === "Backspace" && inp.value === "" && hopData.ids.length > 0) {
          var last = hopData.ids.pop();
          var tags = tagsBox.querySelectorAll(".pf-hop-tag");
          if (tags.length) tags[tags.length - 1].remove();
          renderPfResultsList();
        }
      });

      hopData.addTag = addTag;
    })(i);
  }
}

function showHopSuggestions(hopData, query, suggestEl) {
  suggestEl.innerHTML = "";
  var pos = hopData.pos;
  var existing = {};
  hopData.ids.forEach(function(id) { existing[id] = true; });

  var cities = {};
  pfResults.forEach(function(p) {
    if (pos < p.length && !existing[p[pos]]) cities[p[pos]] = true;
  });
  var ids = Object.keys(cities).sort(function(a, b) {
    return cityName(a).localeCompare(cityName(b));
  });
  if (query) {
    ids = ids.filter(function(id) {
      return cityName(id).toLowerCase().indexOf(query) >= 0 ||
             id.toLowerCase().indexOf(query) >= 0;
    });
  }
  if (ids.length === 0) {
    suggestEl.classList.remove("show");
    return;
  }
  ids.slice(0, 30).forEach(function(id) {
    var opt = document.createElement("div");
    opt.className = "pf-hop-opt";
    opt.textContent = cityName(id) + " (" + id + ")";
    opt.addEventListener("mousedown", function(e) {
      e.preventDefault();
      hopData.addTag(id);
      suggestEl.classList.remove("show");
    });
    suggestEl.appendChild(opt);
  });
  suggestEl.classList.add("show");
}

function applyHopFilters(paths) {
  return paths.filter(function(p) {
    for (var i = 0; i < pfHopFilters.length; i++) {
      var f = pfHopFilters[i];
      if (f.ids.length === 0) continue;
      if (f.pos >= p.length) return false;
      if (f.ids.indexOf(p[f.pos]) < 0) return false;
    }
    return true;
  });
}

function renderPfResults() {
  buildHopFilter();
  renderPfResultsList();
}

function renderPfResultsList() {
  var statusEl = document.getElementById("pf-status");
  var resultsEl = document.getElementById("pf-results");
  var isCycle = document.querySelector('input[name="pf-mode"]:checked').value === "cycles";
  var label = isCycle ? "cycle" : "path";

  var rawN = document.getElementById("pf-n").value.trim();
  var filterN = rawN === "" ? 0 : parseInt(rawN);
  if (isNaN(filterN)) filterN = 0;

  var filtered = pfResults;
  if (filterN > 0) {
    filtered = filtered.filter(function(p) { return p.length - 1 === filterN; });
  }
  filtered = applyHopFilters(filtered);

  if (filtered.length === 0) {
    var msg = pfResults.length === 0 ? "No " + label + "s found" :
      "0 matching (total: " + pfResults.length + ")";
    statusEl.textContent = msg;
    resultsEl.innerHTML = "";
    refreshSelectedPfArcs();
    return;
  }

  var groups = {};
  var lengths = [];
  filtered.forEach(function(path) {
    var n = path.length - 1;
    if (!groups[n]) { groups[n] = []; lengths.push(n); }
    groups[n].push(path);
  });
  lengths.sort(function(a, b) { return a - b; });

  var total = filtered.length;
  var txt = total + " " + label + (total > 1 ? "s" : "");
  if (total < pfResults.length) txt += " (of " + pfResults.length + " total)";
  statusEl.textContent = txt;

  var selKeys = {};
  pfSelectedPaths.forEach(function(p) { selKeys[p.join(">")] = true; });

  resultsEl.innerHTML = "";

  function pathLabel(p) {
    return p.map(function(id) { return cityName(id); }).join(" -> ");
  }

  lengths.forEach(function(n) {
    groups[n].sort(function(a, b) {
      var ca = pathCostEur(a), cb = pathCostEur(b);
      var va = ca ? ca.total : Infinity, vb = cb ? cb.total : Infinity;
      if (va !== vb) return va - vb;
      return pathLabel(a).localeCompare(pathLabel(b));
    });

    var grp = document.createElement("div");
    grp.className = "pf-grp open";

    var hd = document.createElement("div");
    hd.className = "pf-grp-hd";
    var arr = document.createElement("span");
    arr.className = "pf-grp-arr";
    arr.innerHTML = "&#9654;";
    var gcb = document.createElement("input");
    gcb.type = "checkbox";
    var grpPaths = groups[n];
    var allSel = grpPaths.every(function(p) { return selKeys[p.join(">")]; });
    gcb.checked = allSel;
    var hdLbl = document.createElement("span");
    hdLbl.textContent = "Length " + n + " (" + grpPaths.length + ")";
    hd.appendChild(arr);
    hd.appendChild(gcb);
    hd.appendChild(hdLbl);

    var body = document.createElement("div");
    body.className = "pf-grp-body";
    var rowEls = [];
    grpPaths.forEach(function(path) {
      var r = makePfRow(path, selKeys);
      body.appendChild(r);
      rowEls.push(r);
    });

    gcb.addEventListener("click", function(ev) { ev.stopPropagation(); });
    gcb.addEventListener("change", function() {
      grpPaths.forEach(function(path, i) {
        var key = path.join(">");
        var idx = -1;
        for (var j = 0; j < pfSelectedPaths.length; j++) {
          if (pfSelectedPaths[j].join(">") === key) { idx = j; break; }
        }
        if (gcb.checked && idx < 0) {
          pfSelectedPaths.push(path);
          rowEls[i].classList.add("selected");
          rowEls[i].querySelector("input").checked = true;
        } else if (!gcb.checked && idx >= 0) {
          pfSelectedPaths.splice(idx, 1);
          rowEls[i].classList.remove("selected");
          rowEls[i].querySelector("input").checked = false;
        }
      });
      refreshSelectedPfArcs();
    });

    arr.addEventListener("click", function(ev) {
      ev.stopPropagation();
      grp.classList.toggle("open");
    });
    hdLbl.addEventListener("click", function(ev) {
      ev.stopPropagation();
      grp.classList.toggle("open");
    });

    grp.appendChild(hd);
    grp.appendChild(body);
    resultsEl.appendChild(grp);
  });

  refreshSelectedPfArcs();
}

function pathCostEur(path) {
  var total = 0;
  var allKnown = true;
  var today = new Date().toISOString().slice(0, 10);
  var tfStart = tfEnabled ? document.getElementById("tf-start").value : null;
  var tfEnd = tfEnabled ? document.getElementById("tf-end").value : null;
  for (var i = 0; i < path.length - 1; i++) {
    var key = path[i] + "-" + path[i + 1];
    var flights = fareByRoute[key] || [];
    var best = null;
    for (var j = 0; j < flights.length; j++) {
      var f = flights[j];
      if (f.date < today) continue;
      if (tfStart && f.date < tfStart) continue;
      if (tfEnd && f.date > tfEnd) continue;
      if (f.price <= 0 || f.eur <= 0) continue;
      if (best === null || f.eur < best) best = f.eur;
    }
    if (best === null) { allKnown = false; }
    else { total += best; }
  }
  if (!allKnown && total === 0) return null;
  return {total: Math.round(total * 100) / 100, partial: !allKnown};
}

function makePfRow(path, selKeys) {
  var row = document.createElement("div");
  row.className = "pf-row";
  var key = path.join(">");
  var isSel = selKeys && selKeys[key];
  if (isSel) row.classList.add("selected");

  var cb = document.createElement("input");
  cb.type = "checkbox";
  cb.checked = !!isSel;

  var cost = pathCostEur(path);
  var costSpan = document.createElement("span");
  costSpan.className = "pf-row-cost";
  if (cost) {
    costSpan.textContent = (cost.partial ? "~" : "") + cost.total.toFixed(0) + "\u20AC";
    costSpan.title = cost.partial ? "Partial: some legs have no fare data" : "Total cheapest fares in EUR";
  } else {
    costSpan.textContent = "--";
    costSpan.title = "No fare data available";
  }

  var lbl = document.createElement("span");
  lbl.className = "pf-row-lbl";
  row.title = path.join(" -> ") + " (" + (path.length - 1) + " hops)";
  for (var pi = 0; pi < path.length; pi++) {
    if (pi > 0) {
      var sep = document.createElement("span");
      sep.textContent = " \u2192 ";
      sep.style.color = edgeAirlineColor(path[pi - 1], path[pi]);
      sep.style.fontWeight = "bold";
      lbl.appendChild(sep);
    }
    var citySpan = document.createElement("span");
    var nd = nodeMap[path[pi]];
    citySpan.textContent = nd ? (nd.city || nd.name) : path[pi];
    lbl.appendChild(citySpan);
  }

  row.appendChild(cb);
  row.appendChild(costSpan);
  row.appendChild(lbl);

  function doToggle() {
    var wasSelected = row.classList.contains("selected");
    togglePfPath(path, row);
    cb.checked = !wasSelected;
  }

  cb.addEventListener("click", function(ev) { ev.stopPropagation(); });
  cb.addEventListener("change", function() { doToggle(); });
  lbl.addEventListener("click", function(ev) {
    ev.stopPropagation();
    doToggle();
  });
  row.addEventListener("mouseenter", function() {
    if (!row.classList.contains("selected")) {
      showPfArcs([path].concat(pfSelectedPaths));
      row.classList.add("on");
    }
  });
  row.addEventListener("mouseleave", function() {
    row.classList.remove("on");
    refreshSelectedPfArcs();
  });
  return row;
}

function showPfArcs(paths) {
  baseArcStroke = 0.4;
  applyZoomStroke();
  var arcs = [];
  paths.forEach(function(path) {
    for (var i = 0; i < path.length - 1; i++) {
      var from = path[i], to = path[i + 1];
      var f = nodeMap[from], t = nodeMap[to];
      if (!f || !t) continue;
      var c = edgeAirlineColor(from, to);
      var al = edgeAirlineCode(from, to);
      arcs.push({
        startLat: f.lat, startLng: f.lon,
        endLat: t.lat, endLng: t.lon,
        color: c, alt: undefined,
        fromIata: from, toIata: to, airline: al
      });
      addArrowArcs(arcs, f.lat, f.lon, t.lat, t.lon, c, undefined, from, to, al);
    }
  });
  myGlobe.arcsData(arcs);
}

function clearPathfinder() {
  _pfRunning = false;
  pfActive = false;
  pfResults = [];
  pfHitLimit = false;
  pfHighlight = -1;
  pfSelectedPaths = [];
  pfHopFilters = [];
  document.getElementById("pf-hop-filter").innerHTML = "";
  document.getElementById("pf-hop-filter").classList.remove("active");
  document.getElementById("pf-progress").classList.remove("active");
  document.getElementById("pf-status").textContent = "";
  document.getElementById("pf-results").innerHTML = "";
  baseArcStroke = 0.2;
  applyZoomStroke();
  refreshArcs();
}

/* ---- Search ---- */
var searchBox = document.getElementById("search");
var suggestionsEl = document.getElementById("suggestions");

searchBox.addEventListener("input", function() {
  var q = this.value.trim().toLowerCase();
  if (!q) { suggestionsEl.style.display = "none"; return; }
  suggestionsEl.innerHTML = "";

  var countryHits = Object.keys(countryGroups).filter(function(cc) {
    var name = (countryNames[cc] || "").toLowerCase();
    return name.indexOf(q) !== -1 || cc.indexOf(q) !== -1;
  });
  countryHits.forEach(function(cc) {
    var visCities = (countryGroups[cc] || []).filter(function(c) { return isNodeVisible(c.id); });
    if (!visCities.length) return;
    var el = document.createElement("div");
    el.className = "sug sug-country";
    el.innerHTML = '<span class="sug-icon">\uD83C\uDDEA\uD83C\uDDFA</span><b>' +
      (countryNames[cc] || cc.toUpperCase()) + '</b>' +
      ' <span style="color:#484f58">' + visCities.length + " cities</span>";
    el.onmousedown = function(ev) { ev.preventDefault(); };
    el.onclick = function() {
      visCities.forEach(function(c) { activeCities.add(c.id); });
      refreshArcs();
      syncUI();
      suggestionsEl.style.display = "none";
      searchBox.value = "";
      var g = document.querySelector('.cg[data-cc="' + cc + '"]');
      if (g) {
        setTimeout(function() {
          g.scrollIntoView({behavior: "smooth", block: "start"});
        }, 80);
      }
    };
    suggestionsEl.appendChild(el);
  });

  var cityHits = nodesData.filter(function(n) {
    return n.id.toLowerCase().indexOf(q) !== -1 ||
           (n.name || "").toLowerCase().indexOf(q) !== -1 ||
           (n.city || "").toLowerCase().indexOf(q) !== -1;
  }).slice(0, 8);
  cityHits.forEach(function(n) {
    var el = document.createElement("div");
    el.className = "sug";
    el.innerHTML = "<b>" + n.id + "</b> " + (n.city || n.name) +
      ' <span style="color:#484f58">' + (n.country_name || n.country.toUpperCase()) + "</span>";
    el.onmousedown = function(ev) { ev.preventDefault(); };
    el.onclick = function() {
      if (!activeCities.has(n.id)) activeCities.add(n.id);
      refreshArcs();
      syncUI();
      myGlobe.pointOfView({ lat: n.lat, lng: n.lon, altitude: 1.8 }, 800);
      suggestionsEl.style.display = "none";
      searchBox.value = "";
      var row = cityRows[n.id];
      if (row) {
        setTimeout(function() {
          row.scrollIntoView({behavior: "smooth", block: "center"});
          row.classList.add("flash");
          setTimeout(function() { row.classList.remove("flash"); }, 1200);
        }, 80);
      }
    };
    suggestionsEl.appendChild(el);
  });

  if (suggestionsEl.children.length === 0) {
    suggestionsEl.style.display = "none";
  } else {
    suggestionsEl.style.display = "block";
  }
});

searchBox.addEventListener("blur", function() {
  setTimeout(function() { suggestionsEl.style.display = "none"; }, 150);
});

/* ---- Reset / Clear ---- */
function resetView() {
  activeCities.clear();
  intersectMode = false;
  document.getElementById("intersect-cb").checked = false;
  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
  document.querySelectorAll(".cg").forEach(function(g) { g.classList.remove("open"); });
  myGlobe.pointOfView({ lat: 50, lng: 10, altitude: 2.5 }, 800);
  searchBox.value = "";
  ctrl.autoRotate = true;
}

function clearAll() {
  activeCities.clear();
  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
}

document.getElementById("select-all-cb").addEventListener("change", function() {
  var checked = this.checked;
  nodesData.forEach(function(n) {
    if (checked && isNodeVisible(n.id)) activeCities.add(n.id);
    else activeCities.delete(n.id);
  });
  if (pfActive) clearPathfinder();
  refreshArcs();
  syncUI();
});

/* ---- Left column resize ---- */
(function() {
  var col = document.getElementById("left-col");
  var handle = document.getElementById("left-col-resize");
  var dragging = false;
  handle.addEventListener("mousedown", function(e) {
    e.preventDefault();
    dragging = true;
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  });
  document.addEventListener("mousemove", function(e) {
    if (!dragging) return;
    var w = e.clientX - col.getBoundingClientRect().left;
    w = Math.max(380, Math.min(w, window.innerWidth * 0.6));
    col.style.width = w + "px";
  });
  document.addEventListener("mouseup", function() {
    if (!dragging) return;
    dragging = false;
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  });
})();
</script>
</body>
</html>
"""
