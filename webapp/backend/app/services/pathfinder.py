"""DFS path and cycle search over route graph with fare-based cost in EUR."""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.fare import Fare
from app.schemas.search import (
    CycleSearchRequest,
    PathLeg,
    PathResult,
    PathSearchRequest,
    SearchResponse,
)
from app.services.exchange_rates import convert_to_eur, get_rates
from app.services.graph import fetch_filtered_route_edges

AIRLINE_META = {
    "FR": {"name": "Ryanair", "color": "#0b4ea2"},
    "W6": {"name": "Wizz Air", "color": "#e500a4"},
}

TIME_BUDGET_S = 10.0
NODE_CHECK_INTERVAL = 5000


def _normalize_date_range(
    date_from: date | None, date_to: date | None
) -> tuple[date | None, date | None]:
    if date_from is None and date_to is None:
        return None, None
    if date_from is None:
        return date_to, date_to
    if date_to is None:
        return date_from, date_from
    return date_from, date_to


def _sort_key_cost(result: PathResult) -> tuple[int, float]:
    if result.total_cost_eur is None:
        return (1, 0.0)
    return (0, result.total_cost_eur)


def _cycle_dedup_key(airports: list[str]) -> tuple[str, ...]:
    """Normalize cycle by sorting unique airports (drop duplicate closing node)."""
    if len(airports) < 2:
        return tuple(airports)
    if airports[0] == airports[-1]:
        core = airports[:-1]
    else:
        core = airports
    return tuple(sorted(core))


def _build_path_result(
    airports: list[str],
    edges: list[tuple[str, str, str]],
    leg_fares: dict[tuple[str, str], list[tuple[date, float]]],
) -> PathResult:
    total, partial, per_leg = _sequential_best_cost(edges, leg_fares)
    legs: list[PathLeg] = []
    for i, (o, d, airline) in enumerate(edges):
        cost, best_date = per_leg[i]
        legs.append(PathLeg(
            origin=o, destination=d, airline=airline,
            cost_eur=cost, best_date=best_date,
        ))
    return PathResult(
        path=airports,
        legs=legs,
        total_cost_eur=total,
        is_partial=partial,
    )


async def _load_leg_fares_by_date(
    db: AsyncSession,
    legs: set[tuple[str, str, str]],
    date_from: date | None,
    date_to: date | None,
    rates: dict[str, float],
) -> dict[tuple[str, str], list[tuple[date, float]]]:
    """Load all fares per O-D pair, grouped as sorted (date, eur) pairs.

    Keyed by (origin, destination) -- not airline-specific, so the cost
    considers fares from any carrier operating the route.
    """
    if not legs:
        return {}
    od_pairs = list({(o, d) for o, d, _ in legs})
    df, dt = _normalize_date_range(date_from, date_to)

    by_od: dict[tuple[str, str], dict[date, float]] = {od: {} for od in od_pairs}

    BATCH = 500
    for i in range(0, len(od_pairs), BATCH):
        batch = od_pairs[i : i + BATCH]
        stmt = select(
            Fare.origin,
            Fare.destination,
            Fare.departure_date,
            Fare.price,
            Fare.currency,
        ).where(
            tuple_(Fare.origin, Fare.destination).in_(batch),
            Fare.price > 0,
            Fare.departure_date >= func.current_date(),
        )
        if df is not None and dt is not None:
            stmt = stmt.where(Fare.departure_date >= df, Fare.departure_date <= dt)
        result = await db.execute(stmt)

        for o, d, dep_date, price, currency in result.all():
            if price is None or float(price) <= 0 or dep_date is None:
                continue
            eur = convert_to_eur(float(price), str(currency), rates)
            if eur is None:
                continue
            key = (o, d)
            prev = by_od[key].get(dep_date)
            if prev is None or eur < prev:
                by_od[key][dep_date] = eur

    out: dict[tuple[str, str], list[tuple[date, float]]] = {}
    for k, date_map in by_od.items():
        out[k] = sorted(date_map.items(), key=lambda x: (x[1], x[0]))
    return out


def _sequential_best_cost(
    edges: list[tuple[str, str, str]],
    leg_fares: dict[tuple[str, str], list[tuple[date, float]]],
    min_stay_hours: int = 24,
) -> tuple[float | None, bool, list[tuple[float | None, str | None]]]:
    """Find the cheapest sequential trip with min stay between legs.

    Each subsequent leg must depart at least min_stay_hours after the
    previous leg's departure date.

    Returns (total_eur, is_partial, [(cost_per_leg, best_date_per_leg), ...])
    """
    n = len(edges)
    if n == 0:
        return 0.0, False, []

    # Build date-sorted fare lists per leg
    leg_date_fares: list[list[tuple[date, float]]] = []
    for o, d, _ in edges:
        fares = leg_fares.get((o, d), [])
        by_date = sorted(fares, key=lambda x: x[0])
        leg_date_fares.append(by_date)

    # Check if any leg has no fares at all
    missing = [i for i in range(n) if not leg_date_fares[i]]
    if missing:
        per_leg: list[tuple[float | None, str | None]] = []
        partial_total = 0.0
        min_dt: date | None = None
        for i in range(n):
            if not leg_date_fares[i]:
                per_leg.append((None, None))
                continue
            best_eur = None
            best_d = None
            for dt, eur in leg_date_fares[i]:
                if min_dt is not None and dt < min_dt:
                    continue
                if best_eur is None or eur < best_eur:
                    best_eur = eur
                    best_d = dt
            if best_eur is not None and best_d is not None:
                per_leg.append((best_eur, best_d.isoformat()))
                partial_total += best_eur
                min_dt = best_d
            else:
                per_leg.append((None, None))
        return partial_total if partial_total > 0 else None, True, per_leg

    # DP: find minimum total cost with sequential dates
    # dp[i] = dict mapping date -> (min_cost_to_here, chosen_dates_so_far)
    best_total: float | None = None
    best_choices: list[tuple[date, float]] | None = None

    # Iterate through all combinations efficiently:
    # For each fare option of leg 0, try to build cheapest forward
    # Use suffix minimum arrays for speed

    # Build suffix-min for each leg: for each date, what's the cheapest
    # fare on or after that date
    suffix_mins: list[list[tuple[date, float]]] = []
    for fares in leg_date_fares:
        sm: list[tuple[date, float]] = []
        min_so_far = float("inf")
        best_dt = fares[-1][0]
        for dt, eur in reversed(fares):
            if eur < min_so_far:
                min_so_far = eur
                best_dt = dt
            sm.append((best_dt, min_so_far))
        sm.reverse()
        suffix_mins.append(sm)

    import bisect

    def find_cheapest_from(leg_idx: int, min_date_val: date) -> tuple[date, float] | None:
        fares = leg_date_fares[leg_idx]
        dates_only = [f[0] for f in fares]
        pos = bisect.bisect_left(dates_only, min_date_val)
        if pos >= len(fares):
            return None
        sm = suffix_mins[leg_idx]
        return sm[pos]

    stay_days = max(0, (min_stay_hours + 23) // 24)

    # DP: dp[i] maps date -> (min_cost_to_reach, chosen_dates_list)
    # Build from last leg backward using suffix minimums,
    # then forward pass to find best total.

    # Forward recursive search with memoization
    # For each leg, try all dates >= earliest and combine with best suffix
    def solve(
        leg_idx: int, min_date_val: date
    ) -> tuple[float, list[tuple[date, float]]] | None:
        fares = leg_date_fares[leg_idx]
        dates_only = [f[0] for f in fares]
        pos = bisect.bisect_left(dates_only, min_date_val)
        if pos >= len(fares):
            return None
        if leg_idx == n - 1:
            # Last leg: just pick cheapest from pos onward
            sm = suffix_mins[leg_idx]
            best_dt, best_eur = sm[pos]
            return (best_eur, [(best_dt, best_eur)])
        best_result: tuple[float, list[tuple[date, float]]] | None = None
        for j in range(pos, len(fares)):
            dt_j, eur_j = fares[j]
            next_earliest = dt_j + timedelta(days=stay_days)
            sub = solve(leg_idx + 1, next_earliest)
            if sub is None:
                continue
            total = eur_j + sub[0]
            if best_result is None or total < best_result[0]:
                best_result = (total, [(dt_j, eur_j)] + sub[1])
        return best_result

    if leg_date_fares[0]:
        first_date = leg_date_fares[0][0][0]
        for f in leg_date_fares[0]:
            if f[0] < first_date:
                first_date = f[0]
        result = solve(0, first_date)
        if result is not None:
            best_total, best_choices_list = result
            per_leg = [(eur, dt.isoformat()) for dt, eur in best_choices_list]
            return best_total, False, per_leg

    # Fallback: greedy sequential, accepting partial
    per_leg_fb: list[tuple[float | None, str | None]] = [(None, None)] * n
    fb_total = 0.0
    cur_d: date | None = None
    for i in range(n):
        earliest = (cur_d + timedelta(days=stay_days)) if cur_d else None
        r = find_cheapest_from(i, earliest) if earliest else (
            suffix_mins[i][0] if suffix_mins[i] else None
        )
        if r:
            per_leg_fb[i] = (r[1], r[0].isoformat())
            fb_total += r[1]
            cur_d = r[0]
        else:
            per_leg_fb[i] = (None, None)
    return fb_total if fb_total > 0 else None, True, per_leg_fb


def _build_adjacency_with_airlines(
    edges: list[tuple[str, str, str]],
) -> dict[str, list[tuple[str, str]]]:
    adj: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for o, d, a in edges:
        adj[o].append((d, a))
    for o in adj:
        adj[o].sort(key=lambda t: (t[0], t[1]))
    return adj


async def find_paths(db: AsyncSession, request: PathSearchRequest) -> SearchResponse:
    t_start = time.perf_counter()
    edges = await fetch_filtered_route_edges(
        db, request.airline, request.date_from, request.date_to
    )
    rates = await get_rates(db)
    leg_keys = {(o, d, a) for o, d, a in edges}
    leg_fares = await _load_leg_fares_by_date(
        db, leg_keys, request.date_from, request.date_to, rates
    )

    all_nodes: set[str] = set()
    for o, d, _ in edges:
        all_nodes.add(o)
        all_nodes.add(d)

    dest_set = set(request.destinations) if request.destinations is not None else all_nodes
    origins_set = set(request.origins)
    allowed_intermediate = origins_set | dest_set

    adj = _build_adjacency_with_airlines(edges)

    results: list[PathResult] = []
    node_visits = 0
    timed_out = False

    def over_budget() -> bool:
        nonlocal timed_out
        if timed_out:
            return True
        if node_visits % NODE_CHECK_INTERVAL == 0 and node_visits > 0:
            if time.perf_counter() - t_start > TIME_BUDGET_S:
                timed_out = True
                return True
        return False

    def dfs_path(
        current: str,
        path_nodes: list[str],
        path_edges: list[tuple[str, str, str]],
        visited: set[str],
    ) -> None:
        nonlocal node_visits
        node_visits += 1
        if over_budget():
            return
        if current in dest_set and len(path_edges) >= 1:
            interiors_ok = (not request.only_selected) or all(
                x in allowed_intermediate for x in path_nodes[1:-1]
            )
            if interiors_ok:
                results.append(_build_path_result(path_nodes, path_edges, leg_fares))
        if len(path_edges) >= request.max_hops:
            return
        for nxt, airline in adj.get(current, []):
            if over_budget():
                return
            if nxt in visited:
                continue
            next_nodes = path_nodes + [nxt]
            if request.only_selected:
                interiors = next_nodes[1:-1]
                if not all(x in allowed_intermediate for x in interiors):
                    continue
            next_edges = path_edges + [(current, nxt, airline)]
            visited.add(nxt)
            dfs_path(nxt, next_nodes, next_edges, visited)
            visited.remove(nxt)

    for origin in sorted(origins_set):
        if over_budget():
            break
        if origin not in adj and origin not in all_nodes:
            continue
        dfs_path(origin, [origin], [], {origin})

    results = [r for r in results if not r.is_partial]
    results.sort(key=_sort_key_cost)
    elapsed_ms = (time.perf_counter() - t_start) * 1000
    return SearchResponse(
        results=results,
        count=len(results),
        search_time_ms=elapsed_ms,
    )


async def find_cycles(db: AsyncSession, request: CycleSearchRequest) -> SearchResponse:
    t_start = time.perf_counter()
    edges = await fetch_filtered_route_edges(
        db, None, request.date_from, request.date_to
    )
    rates = await get_rates(db)
    leg_keys = {(o, d, a) for o, d, a in edges}
    leg_fares = await _load_leg_fares_by_date(
        db, leg_keys, request.date_from, request.date_to, rates
    )

    origins_set = set(request.origins)
    allowed_intermediate = origins_set

    adj = _build_adjacency_with_airlines(edges)

    seen_cycle_keys: set[tuple[str, ...]] = set()
    results: list[PathResult] = []
    node_visits = 0
    timed_out = False

    def over_budget() -> bool:
        nonlocal timed_out
        if timed_out:
            return True
        if node_visits % NODE_CHECK_INTERVAL == 0 and node_visits > 0:
            if time.perf_counter() - t_start > TIME_BUDGET_S:
                timed_out = True
                return True
        return False

    def dfs_cycle(
        start: str,
        current: str,
        path_nodes: list[str],
        path_edges: list[tuple[str, str, str]],
        visited: set[str],
    ) -> None:
        nonlocal node_visits
        node_visits += 1
        if over_budget():
            return
        for nxt, airline in adj.get(current, []):
            if over_budget():
                return
            if nxt == start and len(path_edges) >= 1:
                total_edges = len(path_edges) + 1
                if total_edges > request.max_hops:
                    continue
                full_nodes = path_nodes + [start]
                if request.only_selected:
                    interiors = full_nodes[1:-1]
                    if not all(x in allowed_intermediate for x in interiors):
                        continue
                key = _cycle_dedup_key(full_nodes)
                if key in seen_cycle_keys:
                    continue
                seen_cycle_keys.add(key)
                full_edges = path_edges + [(current, nxt, airline)]
                results.append(_build_path_result(full_nodes, full_edges, leg_fares))
                continue
            if nxt in visited:
                continue
            if len(path_edges) + 1 >= request.max_hops:
                continue
            if request.only_selected:
                if nxt not in allowed_intermediate:
                    continue
            next_nodes = path_nodes + [nxt]
            next_edges = path_edges + [(current, nxt, airline)]
            visited.add(nxt)
            dfs_cycle(start, nxt, next_nodes, next_edges, visited)
            visited.remove(nxt)

    for start in sorted(origins_set):
        if over_budget():
            break
        if start not in adj:
            continue
        dfs_cycle(start, start, [start], [], {start})

    results = [r for r in results if not r.is_partial]
    results.sort(key=_sort_key_cost)
    elapsed_ms = (time.perf_counter() - t_start) * 1000
    return SearchResponse(
        results=results,
        count=len(results),
        search_time_ms=elapsed_ms,
    )
