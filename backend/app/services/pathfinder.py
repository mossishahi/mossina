"""DFS path and cycle search over route graph with fare-based cost in EUR."""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from mossina_db.models import Fare
from app.schemas.search import (
    CycleSearchRequest,
    HopConstraint,
    LegConstraint,
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
    hop_filters: list[HopConstraint] | None = None,
) -> PathResult:
    stay_days_per_hop = None
    if hop_filters:
        stay_days_per_hop = []
        for i in range(len(edges)):
            hop_idx = i + 1
            hf = hop_filters[hop_idx] if hop_idx < len(hop_filters) else None
            min_d = hf.min_stay_days if hf and hf.min_stay_days is not None else 1
            max_d = hf.max_stay_days if hf and hf.max_stay_days is not None else None
            stay_days_per_hop.append((min_d, max_d))
    total, partial, per_leg = _sequential_best_cost(edges, leg_fares, stay_days_per_hop=stay_days_per_hop)
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
    stay_days_per_hop: list[tuple[int, int | None]] | None = None,
) -> tuple[float | None, bool, list[tuple[float | None, str | None]]]:
    """Find the cheapest sequential trip with min/max stay between legs.

    stay_days_per_hop: list of (min_days, max_days) per intermediate stop.
    If None, falls back to min_stay_hours globally.

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

    default_stay = max(0, (min_stay_hours + 23) // 24)

    def _stay_for_leg(leg_idx: int) -> tuple[int, int | None]:
        """Return (min_days, max_days) for the gap after leg_idx."""
        if stay_days_per_hop and leg_idx < len(stay_days_per_hop):
            return stay_days_per_hop[leg_idx]
        return (default_stay, None)

    def solve(
        leg_idx: int, min_date_val: date, max_date_val: date | None = None,
    ) -> tuple[float, list[tuple[date, float]]] | None:
        fares = leg_date_fares[leg_idx]
        dates_only = [f[0] for f in fares]
        pos = bisect.bisect_left(dates_only, min_date_val)
        if pos >= len(fares):
            return None
        if leg_idx == n - 1:
            sm = suffix_mins[leg_idx]
            if max_date_val is not None:
                best_dt, best_eur = None, float("inf")
                for j in range(pos, len(fares)):
                    if fares[j][0] > max_date_val:
                        break
                    if fares[j][1] < best_eur:
                        best_eur = fares[j][1]
                        best_dt = fares[j][0]
                if best_dt is None:
                    return None
                return (best_eur, [(best_dt, best_eur)])
            best_dt, best_eur = sm[pos]
            return (best_eur, [(best_dt, best_eur)])
        best_result: tuple[float, list[tuple[date, float]]] | None = None
        min_stay, max_stay = _stay_for_leg(leg_idx)
        end = len(fares)
        if max_date_val is not None:
            end = bisect.bisect_right(dates_only, max_date_val)
        for j in range(pos, end):
            dt_j, eur_j = fares[j]
            next_earliest = dt_j + timedelta(days=min_stay)
            next_latest = (dt_j + timedelta(days=max_stay)) if max_stay is not None else None
            sub = solve(leg_idx + 1, next_earliest, next_latest)
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
        min_s, _ = _stay_for_leg(i - 1) if i > 0 else (default_stay, None)
        earliest = (cur_d + timedelta(days=min_s)) if cur_d else None
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


def _hop_ok(city: str, hop_idx: int, hop_filters: list[HopConstraint] | None) -> bool:
    """Check if a city is allowed at hop position hop_idx."""
    if not hop_filters or hop_idx >= len(hop_filters):
        return True
    hf = hop_filters[hop_idx]
    if hf.include_cities and city not in hf.include_cities:
        return False
    if hf.exclude_cities and city in hf.exclude_cities:
        return False
    return True


def _leg_ok(airline: str, leg_idx: int, leg_filters: list[LegConstraint] | None) -> bool:
    """Check if an airline is allowed on leg at position leg_idx."""
    if not leg_filters or leg_idx >= len(leg_filters):
        return True
    lf = leg_filters[leg_idx]
    if lf.airline and airline != lf.airline:
        return False
    return True


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
    found_at_depth: set[tuple[str, ...]] = set()

    def over_budget() -> bool:
        nonlocal timed_out
        if timed_out:
            return True
        if node_visits % NODE_CHECK_INTERVAL == 0 and node_visits > 0:
            if time.perf_counter() - t_start > TIME_BUDGET_S:
                timed_out = True
                return True
        return False

    hop_filters = request.hop_filters
    leg_filters = request.leg_filters

    def dfs_path(
        current: str,
        path_nodes: list[str],
        path_edges: list[tuple[str, str, str]],
        visited: set[str],
        depth_limit: int,
    ) -> None:
        nonlocal node_visits
        node_visits += 1
        if over_budget():
            return
        if current in dest_set and len(path_edges) >= 1:
            if len(path_edges) == depth_limit:
                sig = tuple(path_nodes)
                if sig not in found_at_depth:
                    last_hop_idx = len(path_nodes) - 1
                    if _hop_ok(current, last_hop_idx, hop_filters):
                        interiors_ok = (not request.only_selected) or all(
                            x in allowed_intermediate for x in path_nodes[1:-1]
                        )
                        if interiors_ok:
                            found_at_depth.add(sig)
                            results.append(_build_path_result(path_nodes, path_edges, leg_fares, hop_filters))
        if len(path_edges) >= depth_limit:
            return
        leg_idx = len(path_edges)
        for nxt, airline in adj.get(current, []):
            if over_budget():
                return
            if nxt in visited:
                continue
            if not _leg_ok(airline, leg_idx, leg_filters):
                continue
            nxt_hop_idx = len(path_nodes)
            if not _hop_ok(nxt, nxt_hop_idx, hop_filters):
                continue
            next_nodes = path_nodes + [nxt]
            if request.only_selected:
                interiors = next_nodes[1:-1]
                if not all(x in allowed_intermediate for x in interiors):
                    continue
            next_edges = path_edges + [(current, nxt, airline)]
            visited.add(nxt)
            dfs_path(nxt, next_nodes, next_edges, visited, depth_limit)
            visited.remove(nxt)

    # max_stops counts cities visited; convert to edge count for DFS.
    max_edges = max(1, request.max_stops - 1)
    for depth in range(1, max_edges + 1):
        if over_budget():
            break
        for origin in sorted(origins_set):
            if over_budget():
                break
            if origin not in adj and origin not in all_nodes:
                continue
            if not _hop_ok(origin, 0, hop_filters):
                continue
            dfs_path(origin, [origin], [], {origin}, depth)

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

    hop_filters = request.hop_filters
    leg_filters = request.leg_filters

    def dfs_cycle(
        start: str,
        current: str,
        path_nodes: list[str],
        path_edges: list[tuple[str, str, str]],
        visited: set[str],
        depth_limit: int,
    ) -> None:
        nonlocal node_visits
        node_visits += 1
        if over_budget():
            return
        leg_idx = len(path_edges)
        for nxt, airline in adj.get(current, []):
            if over_budget():
                return
            if not _leg_ok(airline, leg_idx, leg_filters):
                continue
            if nxt == start and len(path_edges) >= 1:
                total_edges = len(path_edges) + 1
                if total_edges != depth_limit:
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
                results.append(_build_path_result(full_nodes, full_edges, leg_fares, hop_filters))
                continue
            if nxt in visited:
                continue
            nxt_hop_idx = len(path_nodes)
            if not _hop_ok(nxt, nxt_hop_idx, hop_filters):
                continue
            if len(path_edges) + 1 >= depth_limit:
                continue
            if request.only_selected:
                if nxt not in allowed_intermediate:
                    continue
            next_nodes = path_nodes + [nxt]
            next_edges = path_edges + [(current, nxt, airline)]
            visited.add(nxt)
            dfs_cycle(start, nxt, next_nodes, next_edges, visited, depth_limit)
            visited.remove(nxt)

    # For cycles, max_stops includes both endpoints (which are the same
    # city), so a cycle visiting 3 distinct cities (A->B->C->A) has
    # max_stops=4 but exactly 3 edges. Edge count = max_stops - 1.
    max_edges = max(2, request.max_stops - 1)
    for depth in range(2, max_edges + 1):
        if over_budget():
            break
        for start in sorted(origins_set):
            if over_budget():
                break
            if start not in adj:
                continue
            if not _hop_ok(start, 0, hop_filters):
                continue
            dfs_cycle(start, start, [start], [], {start}, depth)

    results = [r for r in results if not r.is_partial]
    results.sort(key=_sort_key_cost)
    elapsed_ms = (time.perf_counter() - t_start) * 1000
    return SearchResponse(
        results=results,
        count=len(results),
        search_time_ms=elapsed_ms,
    )
