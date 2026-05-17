import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import type { CSSProperties } from "react";
import { createPortal } from "react-dom";
import { ChevronRight, SlidersHorizontal, Check, MapPin, RotateCcw } from "lucide-react";
import { List as VList } from "react-window";
import { useSearchPaths, useSearchCycles } from "@/hooks/useSearch";
import { useAirports } from "@/hooks/useAirports";
import { useMapStore } from "@/stores/mapStore";
import { useFilterStore } from "@/stores/filterStore";
import { usePathStore, pathKey_ } from "@/stores/pathStore";
import { AIRLINE_META } from "@/api/types";
import type { PathResult } from "@/api/types";
import SearchControls from "./SearchControls";
import RangeSlider from "./RangeSlider";

// ----- Filter model ----------------------------------------------------

// Stay slider hard limits. Defaults are what the slider starts at; bounds
// are the slider's full sweep.
const DAYS_MIN = 1;
const DAYS_MAX = 14;
const DEFAULT_MIN_DAYS = 1;
const DEFAULT_MAX_DAYS = 4;

// Virtualization config for the path lists inside each Stops group.
// Each PathCard renders in one horizontal line (overflow-x-auto handles
// the long itineraries), so the row height is constant. The 4px we add
// on top of the card height plays the role of `space-y-1` between rows.
const ROW_HEIGHT = 36;
const ROW_PADDING_Y = 2;
const MAX_LIST_HEIGHT = 480;

interface HopFilterValue {
  minDays: number;
  maxDays: number;
  includeCities: string[];
  excludeCities: string[];
}

interface GroupFilters {
  onlySelected: boolean;
  onlyRoundTrips: boolean;
  hops: HopFilterValue[]; // length = stop count = legs.length + 1
}

const emptyHop = (): HopFilterValue => ({
  minDays: DEFAULT_MIN_DAYS,
  maxDays: DEFAULT_MAX_DAYS,
  includeCities: [],
  excludeCities: [],
});

function defaultGroupFilters(legCount: number): GroupFilters {
  return {
    onlySelected: false,
    onlyRoundTrips: false,
    hops: Array.from({ length: legCount + 1 }, emptyHop),
  };
}

// "No constraint" state. Used by Reset and by the active-filter check.
function neutralGroupFilters(legCount: number): GroupFilters {
  return {
    onlySelected: false,
    onlyRoundTrips: false,
    hops: Array.from({ length: legCount + 1 }, () => ({
      minDays: DAYS_MIN,
      maxDays: DAYS_MAX,
      includeCities: [],
      excludeCities: [],
    })),
  };
}

// A "round trip" (cycle) is a path whose origin equals its destination.
function isRoundTrip(p: PathResult): boolean {
  return p.path.length > 1 && p.path[0] === p.path[p.path.length - 1];
}

// Is anything constraining the result list? Compared against the slider's
// full sweep [DAYS_MIN, DAYS_MAX], not the visual default [1, 4]. So
// applying defaults still counts as "active".
function isGroupFilterActive(f: GroupFilters): boolean {
  if (f.onlySelected || f.onlyRoundTrips) return true;
  return f.hops.some((h, i) => {
    if (h.includeCities.length > 0 || h.excludeCities.length > 0) return true;
    const isIntermediate = i > 0 && i < f.hops.length - 1;
    if (isIntermediate && (h.minDays > DAYS_MIN || h.maxDays < DAYS_MAX)) return true;
    return false;
  });
}

// Does any stop constrain the stay duration (away from "any duration")?
// We treat [DAYS_MIN, DAYS_MAX] as "no constraint".
function hopsHaveDayConstraints(hops: HopFilterValue[]): boolean {
  return hops.some(
    (h, i) =>
      // Only intermediates contribute -- endpoints don't stay
      i > 0 && i < hops.length - 1 &&
      (h.minDays !== DAYS_MIN || h.maxDays !== DAYS_MAX),
  );
}

// ----- Pathfinder ------------------------------------------------------

export default function Pathfinder() {
  // Max cities visited (origin + intermediates + destination). Default 4
  // matches the previous behaviour of max_hops=3 (3 edges = 4 stops).
  const [maxStops, setMaxStops] = useState(4);
  const [groupFilters, setGroupFilters] = useState<Record<number, GroupFilters>>({});
  const [openGroups, setOpenGroups] = useState<Set<number>>(new Set());
  const selectedCities = useMapStore((s) => s.selectedCities);
  const activeAirlines = useMapStore((s) => s.activeAirlines);
  const { dateFrom, dateTo } = useFilterStore();
  const groundDistanceKm = useFilterStore((s) => s.groundDistanceKm);

  const { data: airports = [] } = useAirports();
  const nameMap = useMemo(() => {
    const m = new Map<string, string>();
    airports.forEach((a) => m.set(a.iata, a.city || a.name));
    return m;
  }, [airports]);

  const clearPaths = usePathStore((s) => s.clearPaths);
  const setSearchActive = usePathStore((s) => s.setSearchActive);
  const updateSelectedResults = usePathStore((s) => s.updateSelectedResults);

  const pathMutation = useSearchPaths();
  const cycleMutation = useSearchCycles();
  const lastSearchRef = useRef<{ from: string; to: string; cities: string[]; airline?: string } | null>(null);

  const prevCitiesRef = useRef(selectedCities);
  useEffect(() => {
    if (prevCitiesRef.current !== selectedCities) {
      prevCitiesRef.current = selectedCities;
      clearPaths();
      setSearchActive(false);
      pathMutation.reset();
      cycleMutation.reset();
      setGroupFilters({});
      setOpenGroups(new Set());
    }
  }, [selectedCities]);

  function handleSearch() {
    const cities = [...selectedCities];
    if (cities.length === 0) return;

    const from = dateFrom || new Date().toISOString().slice(0, 10);
    const to =
      dateTo ||
      new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

    setOpenGroups(new Set());
    setGroupFilters({});
    clearPaths();
    setSearchActive(true);

    const al = activeAirlines.size === 1 ? [...activeAirlines][0] : undefined;
    lastSearchRef.current = { from, to, cities, airline: al };

    pathMutation.mutate({
      origins: cities,
      destinations: cities,
      max_stops: maxStops,
      date_from: from,
      date_to: to,
      only_selected: false,
      airline: al,
      ground_distance_km: groundDistanceKm > 0 ? groundDistanceKm : null,
    });

    cycleMutation.mutate({
      origins: cities,
      max_stops: maxStops,
      date_from: from,
      date_to: to,
      only_selected: false,
      ground_distance_km: groundDistanceKm > 0 ? groundDistanceKm : null,
    });
  }

  const pathResults = pathMutation.data?.results ?? [];
  const cycleResults = cycleMutation.data?.results ?? [];
  const pathTimeMs = pathMutation.data?.search_time_ms;
  const cycleTimeMs = cycleMutation.data?.search_time_ms;
  const searchTimeMs =
    pathTimeMs != null || cycleTimeMs != null
      ? Math.max(pathTimeMs ?? 0, cycleTimeMs ?? 0)
      : undefined;

  const results = useMemo(
    () => [...pathResults, ...cycleResults],
    [pathResults, cycleResults],
  );

  const anyPending = pathMutation.isPending || cycleMutation.isPending;
  const anySuccess = pathMutation.isSuccess || cycleMutation.isSuccess;
  const anyError = pathMutation.isError && cycleMutation.isError;

  const grouped = useMemo(() => {
    const g: Record<number, PathResult[]> = {};
    results.forEach((p) => {
      const n = p.legs.length;
      if (!g[n]) g[n] = [];
      g[n].push(p);
    });
    Object.values(g).forEach((arr) =>
      arr.sort((a, b) => (a.total_cost_eur ?? Infinity) - (b.total_cost_eur ?? Infinity)),
    );
    return g;
  }, [results]);

  const stopGroups = Object.keys(grouped).map(Number).sort((a, b) => a - b);

  // Apply all per-group filters (booleans + hops include/exclude).
  // Day constraints are NOT applied client-side -- those need the
  // backend to reprice. See applyDayRepricing below.
  const applyGroupFilters = useCallback(
    (paths: PathResult[], stops: number): PathResult[] => {
      const f = groupFilters[stops];
      if (!f) return paths;
      let out = paths;
      if (f.onlyRoundTrips) out = out.filter(isRoundTrip);
      if (f.onlySelected && selectedCities.size > 0) {
        out = out.filter((p) => p.path.every((iata) => selectedCities.has(iata)));
      }
      out = out.filter((p) => {
        for (let i = 0; i < p.path.length; i++) {
          const h = f.hops[i];
          if (!h) continue;
          if (h.includeCities.length > 0 && !h.includeCities.includes(p.path[i])) return false;
          if (h.excludeCities.includes(p.path[i])) return false;
        }
        return true;
      });
      return out;
    },
    [groupFilters, selectedCities],
  );

  const totalFiltered = useMemo(
    () => stopGroups.reduce((sum, n) => sum + applyGroupFilters(grouped[n] || [], n).length, 0),
    [stopGroups, grouped, applyGroupFilters],
  );

  const anyGroupFilterActive = useMemo(
    () => Object.values(groupFilters).some(isGroupFilterActive),
    [groupFilters],
  );

  // Auto-expand the first stops group when a new result set arrives.
  useMemo(() => {
    if (stopGroups.length > 0 && openGroups.size === 0) {
      setOpenGroups(new Set([stopGroups[0]]));
    }
  }, [results]);

  function toggleGroup(n: number) {
    setOpenGroups((prev) => {
      const next = new Set(prev);
      if (next.has(n)) next.delete(n);
      else next.add(n);
      return next;
    });
  }

  function cityName(iata: string) {
    return nameMap.get(iata) || iata;
  }

  const [repricing, setRepricing] = useState(false);

  // When a group's filters are applied with day constraints, re-fetch the
  // search with hop_filters so the backend can reprice with new stay rules.
  const applyDayRepricing = useCallback(
    (filters: GroupFilters) => {
      if (!lastSearchRef.current) return;
      if (!hopsHaveDayConstraints(filters.hops)) return;
      const s = lastSearchRef.current;
      const hopConstraints = filters.hops.map((h) => ({
        min_stay_days: h.minDays,
        max_stay_days: h.maxDays,
        include_cities: h.includeCities.length > 0 ? h.includeCities : null,
        exclude_cities: h.excludeCities.length > 0 ? h.excludeCities : null,
      }));
      setRepricing(true);
      const gdkm = groundDistanceKm > 0 ? groundDistanceKm : null;
      pathMutation.mutate(
        {
          origins: s.cities,
          destinations: s.cities,
          max_stops: maxStops,
          date_from: s.from,
          date_to: s.to,
          only_selected: false,
          airline: s.airline,
          hop_filters: hopConstraints,
          ground_distance_km: gdkm,
        } as any,
        {
          onSuccess: (data) => {
            if (data?.results) updateSelectedResults(data.results);
          },
        },
      );
      cycleMutation.mutate(
        {
          origins: s.cities,
          max_stops: maxStops,
          date_from: s.from,
          date_to: s.to,
          only_selected: false,
          hop_filters: hopConstraints,
          ground_distance_km: gdkm,
        } as any,
        {
          onSuccess: (data) => {
            if (data?.results) updateSelectedResults(data.results);
          },
          onSettled: () => setRepricing(false),
        },
      );
    },
    [maxStops, groundDistanceKm],
  );

  return (
    <>
      <SearchControls
        maxStops={maxStops}
        setMaxStops={setMaxStops}
        onSearch={handleSearch}
        pathPending={pathMutation.isPending}
        cyclePending={cycleMutation.isPending}
        pathDone={pathMutation.isSuccess}
        cycleDone={cycleMutation.isSuccess}
      />

      <div className="bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden pointer-events-auto">
        <div className="px-3 py-2 space-y-1.5">
          {anyError && (
            <div className="text-xs text-[#e5534b] bg-[#e5534b]/10 border border-[#e5534b]/30 rounded-md p-2.5">
              Search failed. Please try again.
            </div>
          )}

          {anyPending && (
            <p className="text-xs text-[#8b949e] text-center py-3">
              Few seconds please<span className="loading-dots" />
            </p>
          )}

          {results.length > 0 && (
            <div className={`relative ${repricing ? "opacity-50 pointer-events-none" : ""}`}>
              {repricing && (
                <div className="absolute inset-0 z-10 flex items-center justify-center">
                  <span className="text-[10px] text-[#58a6ff] bg-[#0d1117]/80 px-3 py-1 rounded-full">Recalculating prices…</span>
                </div>
              )}
              <div className="pb-2 space-y-1.5">
                <p className="text-[11px] text-[#484f58]">
                  {anyGroupFilterActive
                    ? `${totalFiltered} of ${results.length}`
                    : `${results.length}`} trips
                  {searchTimeMs != null && ` in ${(searchTimeMs / 1000).toFixed(1)}s`}
                </p>
              </div>

              {stopGroups.map((n) => {
                const allPaths = grouped[n];
                const groupFiltered = applyGroupFilters(allPaths, n);
                const isOpen = openGroups.has(n);
                const filtersValue = groupFilters[n] || defaultGroupFilters(n);
                return (
                  <div key={n}>
                    <div className="w-full flex items-center gap-2 px-1 py-1.5 border-t border-[#21262d]">
                      <button
                        onClick={() => toggleGroup(n)}
                        className="flex items-center gap-2 flex-1 text-left -mx-1 px-1 py-0.5 rounded hover:bg-[#161b22]/60 transition-colors"
                      >
                        <ChevronRight
                          size={12}
                          className={`text-[#8b949e] transition-transform ${isOpen ? "rotate-90" : ""}`}
                        />
                        <span className="text-xs font-semibold text-[#58a6ff]">
                          {n + 1} Stops
                        </span>
                        <span className="text-[10px] text-[#484f58] bg-[#161b22] px-1.5 py-0.5 rounded-full">
                          {groupFiltered.length !== allPaths.length
                            ? `${groupFiltered.length}/${allPaths.length}`
                            : `${allPaths.length}`}
                        </span>
                      </button>
                      <FilterMenu
                        legCount={n}
                        value={filtersValue}
                        onApply={(next) => {
                          setGroupFilters((prev) => ({ ...prev, [n]: next }));
                          applyDayRepricing(next);
                        }}
                      />
                    </div>

                    {isOpen && (
                      <div className="pb-1">
                        {groupFiltered.length === 0 ? (
                          <p className="text-[10px] text-[#484f58] text-center py-2">No matches</p>
                        ) : (
                          <VList<PathRowExtraProps>
                            rowComponent={PathRow}
                            rowCount={groupFiltered.length}
                            rowHeight={ROW_HEIGHT}
                            rowProps={{
                              items: groupFiltered,
                              cityName,
                              hops: filtersValue.hops,
                            }}
                            overscanCount={6}
                            style={{
                              height: Math.min(
                                groupFiltered.length * ROW_HEIGHT,
                                MAX_LIST_HEIGHT,
                              ),
                            }}
                          />
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {anySuccess && totalFiltered === 0 && (
            <p className="text-xs text-[#8b949e] text-center py-2">
              {results.length > 0
                ? "No results match the current filters"
                : "No trips found"}
            </p>
          )}

          {!anySuccess && !anyPending && !anyError && (
            <p className="text-xs text-[#484f58] text-center py-2">
              Select cities and press Search
            </p>
          )}
        </div>
      </div>
    </>
  );
}

// ----- PathRow (virtualized row wrapper for react-window v2) -----------

// Props that we pass via the List's `rowProps`. react-window will merge in
// `index` + `style` + aria attributes automatically.
interface PathRowExtraProps {
  items: PathResult[];
  cityName: (iata: string) => string;
  hops: HopFilterValue[];
}

interface PathRowInjectedProps {
  index: number;
  style: CSSProperties;
}

function PathRow({
  index,
  style,
  items,
  cityName,
  hops,
}: PathRowExtraProps & PathRowInjectedProps) {
  const p = items[index];
  return (
    <div
      style={{
        ...style,
        paddingTop: ROW_PADDING_Y,
        paddingBottom: ROW_PADDING_Y,
      }}
    >
      <PathCard result={p} cityName={cityName} pathKey={pathKey_(p)} hops={hops} />
    </div>
  );
}

// ----- PathCard --------------------------------------------------------

function PathCard({
  result,
  cityName,
  pathKey,
  hops,
}: {
  result: PathResult;
  cityName: (iata: string) => string;
  pathKey: string;
  hops: HopFilterValue[];
}) {
  const selectedPaths = usePathStore((s) => s.selectedPaths);
  const togglePath = usePathStore((s) => s.togglePath);
  const setMinDays = usePathStore((s) => s.setMinDays);
  const autoSelectBestDates = usePathStore((s) => s.autoSelectBestDates);
  const isSelected = selectedPaths.some((p) => pathKey_(p) === pathKey);

  const cost = result.total_cost_eur;
  const costLabel =
    cost != null
      ? `${result.is_partial ? "~" : ""}${Math.round(cost)}\u20AC`
      : "--";

  function handlePriceClick(e: React.MouseEvent) {
    e.stopPropagation();
    if (!isSelected) {
      togglePath(result);
      setMinDays(pathKey, hops.map((h) => h.minDays * 24));
    }
    autoSelectBestDates(pathKey, result);
  }

  return (
    <div
      onClick={() => {
        togglePath(result);
        if (!isSelected) {
          setMinDays(pathKey, hops.map((h) => h.minDays * 24));
        }
      }}
      className={`group rounded-lg px-2.5 py-2 transition-all cursor-pointer overflow-x-auto border ${
        isSelected
          ? "bg-[#58a6ff]/10 border-[#58a6ff]/30"
          : "bg-[#0d1117] hover:bg-[#161b22] border-[#21262d] hover:border-[#30363d]"
      }`}
    >
      <div className="flex items-center gap-2 w-max">
        <span
          onClick={handlePriceClick}
          className="text-xs font-bold text-[#3fb950] tabular-nums hover:underline hover:text-[#56d364] cursor-pointer"
          title="Click to highlight cheapest dates"
        >
          {costLabel}
        </span>

        {result.path.map((iata, i) => {
          const leg = i < result.legs.length ? result.legs[i] : null;
          const isGround = leg?.kind === "ground";
          const color = isGround
            ? "#8b949e"
            : leg
              ? AIRLINE_META[leg.airline]?.color || "#8b949e"
              : "#8b949e";
          const title = isGround
            ? `Ground transfer (~${Math.round(leg!.ground_distance_km ?? 0)} km)`
            : undefined;
          return (
            <span key={i} className="inline-flex items-center gap-0.5">
              <span
                className="text-[10px] text-[#c9d1d9] font-medium whitespace-nowrap"
                title={`${cityName(iata)} (${iata})`}
              >
                {cityName(iata)}
              </span>
              {i < result.path.length - 1 && (
                <svg
                  width="20"
                  height="8"
                  viewBox="0 0 20 8"
                  className="shrink-0 mx-0.5"
                  style={{ color }}
                  aria-label={title}
                >
                  <line
                    x1="0" y1="4" x2="14" y2="4"
                    stroke="currentColor"
                    strokeWidth="1.2"
                    strokeDasharray={isGround ? "2 2" : undefined}
                  />
                  <path
                    d="M12 1 L17 4 L12 7"
                    stroke="currentColor"
                    strokeWidth="1.2"
                    fill="none"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              )}
            </span>
          );
        })}
      </div>
    </div>
  );
}

// ----- FilterMenu (group-level filter popover) -------------------------

// Fixed popover width (px). Used for placement math.
const POPOVER_WIDTH = 340;
const POPOVER_MARGIN = 4;

function FilterMenu({
  legCount,
  value,
  onApply,
}: {
  legCount: number;
  value: GroupFilters;
  onApply: (next: GroupFilters) => void;
}) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<GroupFilters>(value);
  // Popover anchor position (viewport coordinates). null = closed/unmeasured.
  const [pos, setPos] = useState<{
    top: number;
    bottom: number;
    right: number;
    placement: "below" | "above";
  } | null>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  const active = isGroupFilterActive(value);

  function measure(): { top: number; bottom: number; right: number; placement: "below" | "above" } | null {
    const btn = buttonRef.current;
    if (!btn) return null;
    const rect = btn.getBoundingClientRect();
    // Rough popover height estimate: header (~70px) + per-stop label (~20px)
    //   + (legCount+1) rows (~32px each) + padding (~30px).
    const estHeight = 70 + 20 + (legCount + 1) * 32 + 30;
    const spaceBelow = window.innerHeight - rect.bottom;
    const spaceAbove = rect.top;
    const placement = spaceBelow < estHeight && spaceAbove > spaceBelow ? "above" : "below";
    return {
      top: rect.bottom + POPOVER_MARGIN,
      bottom: window.innerHeight - rect.top + POPOVER_MARGIN,
      right: Math.max(POPOVER_MARGIN, window.innerWidth - rect.right),
      placement,
    };
  }

  function openMenu(e: React.MouseEvent) {
    e.stopPropagation();
    setDraft(value);
    setPos(measure());
    setOpen(true);
  }

  function apply() {
    onApply(draft);
    setOpen(false);
  }

  // Reset: clear every filter for this group. Sets the slider to the full
  // [DAYS_MIN, DAYS_MAX] sweep (i.e., "no constraint") so the funnel icon
  // also goes inactive.
  function reset() {
    const neutral = neutralGroupFilters(legCount);
    setDraft(neutral);
    onApply(neutral);
    setOpen(false);
  }

  // Close on outside click. Checks both the trigger button and the
  // portal-rendered popover (which is outside this component's DOM tree).
  useEffect(() => {
    if (!open) return;
    function onDocClick(e: MouseEvent) {
      const t = e.target as Node;
      if (buttonRef.current && buttonRef.current.contains(t)) return;
      if (popoverRef.current && popoverRef.current.contains(t)) return;
      setOpen(false);
    }
    // Close on outside scrolls (ancestor scroll containers) only -- if the
    // scroll target is INSIDE the popover (its own internal overflow), we
    // leave it open.
    function onScroll(e: Event) {
      const t = e.target as Node | null;
      if (t && popoverRef.current && popoverRef.current.contains(t)) return;
      setOpen(false);
    }
    function onResize() {
      setOpen(false);
    }
    document.addEventListener("mousedown", onDocClick);
    window.addEventListener("resize", onResize);
    // Capture-phase so we catch scrolls in ancestor scroll containers
    // (e.g., the sidebar's overflow-y-auto wrapper).
    window.addEventListener("scroll", onScroll, true);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      window.removeEventListener("resize", onResize);
      window.removeEventListener("scroll", onScroll, true);
    };
  }, [open]);

  function setHop(i: number, hop: HopFilterValue) {
    setDraft((d) => {
      const next = [...d.hops];
      next[i] = hop;
      return { ...d, hops: next };
    });
  }

  const popover = open && pos ? (
    <div
      ref={popoverRef}
      style={{
        position: "fixed",
        right: pos.right,
        top: pos.placement === "below" ? pos.top : "auto",
        bottom: pos.placement === "above" ? pos.bottom : "auto",
        width: POPOVER_WIDTH,
        maxHeight: pos.placement === "below"
          ? window.innerHeight - pos.top - 8
          : window.innerHeight - pos.bottom - 8,
        overflowY: "auto",
      }}
      className="z-50 bg-[#161b22] border border-[#30363d] rounded-lg shadow-xl p-3"
      onMouseDown={(e) => e.stopPropagation()}
      onClick={(e) => e.stopPropagation()}
    >
      <div className="flex items-start gap-2">
        <div className="flex-1 min-w-0">
          <label className="flex items-center gap-2 cursor-pointer select-none px-1 py-1">
            <input
              type="checkbox"
              checked={draft.onlySelected}
              onChange={(e) => setDraft({ ...draft, onlySelected: e.target.checked })}
              className="accent-[#58a6ff] w-3 h-3 rounded"
            />
            <span className="text-[11px] text-[#c9d1d9]">Only selected cities</span>
          </label>
          <label className="flex items-center gap-2 cursor-pointer select-none px-1 py-1">
            <input
              type="checkbox"
              checked={draft.onlyRoundTrips}
              onChange={(e) => setDraft({ ...draft, onlyRoundTrips: e.target.checked })}
              className="accent-[#58a6ff] w-3 h-3 rounded"
            />
            <span className="text-[11px] text-[#c9d1d9]">Same origin and destination only</span>
          </label>
        </div>
        <div className="shrink-0 flex items-center gap-1">
          <button
            onClick={reset}
            className="px-1.5 py-1 text-[10px] font-semibold text-[#8b949e] hover:text-[#e5534b] inline-flex items-center gap-1"
            title="Reset all filters in this list"
            aria-label="Reset filters for this group"
          >
            <RotateCcw size={11} />
            Reset
          </button>
          <button
            onClick={apply}
            className="px-1.5 py-1 text-[10px] font-semibold text-[#3fb950] hover:text-[#56d364] inline-flex items-center gap-1"
            title="Apply all filters"
          >
            <Check size={12} />
            Apply
          </button>
        </div>
      </div>

      <div className="mt-2 pt-2 border-t border-[#21262d]">
        <p className="text-[10px] text-[#484f58] uppercase tracking-wider mb-1.5 px-1">
          Per-stop filters
        </p>
        <div className="space-y-1">
          {draft.hops.map((hop, i) => {
            const isEndpoint = i === 0 || i === legCount;
            return (
              <HopRow
                key={`hop-${i}`}
                hop={hop}
                isEndpoint={isEndpoint}
                onChange={(h) => setHop(i, h)}
              />
            );
          })}
        </div>
      </div>
    </div>
  ) : null;

  return (
    <div className="relative shrink-0">
      <button
        ref={buttonRef}
        onClick={openMenu}
        className={`p-1 rounded transition-colors ${
          active
            ? "text-[#58a6ff] hover:text-[#79c0ff]"
            : "text-[#484f58] hover:text-[#8b949e]"
        }`}
        title="Filter this group"
        aria-label="Filter this group"
      >
        <SlidersHorizontal size={13} strokeWidth={2.2} />
      </button>
      {popover && createPortal(popover, document.body)}
    </div>
  );
}

// ----- HopRow ----------------------------------------------------------

function HopRow({
  hop,
  isEndpoint,
  onChange,
}: {
  hop: HopFilterValue;
  isEndpoint: boolean;
  onChange: (h: HopFilterValue) => void;
}) {
  return (
    <div className="flex items-center gap-1 px-1 py-1">
      <MapPin size={11} strokeWidth={2.2} className="text-[#8b949e] shrink-0" />
      <TagInput
        placeholder="include"
        tags={hop.includeCities}
        color="#3fb950"
        onChange={(includeCities) => onChange({ ...hop, includeCities })}
      />
      <TagInput
        placeholder="exclude"
        tags={hop.excludeCities}
        color="#e5534b"
        onChange={(excludeCities) => onChange({ ...hop, excludeCities })}
      />
      {!isEndpoint && (
        <div className="flex items-center gap-1.5 min-w-[110px]">
          <RangeSlider
            min={DAYS_MIN}
            max={DAYS_MAX}
            value={[hop.minDays, hop.maxDays]}
            onChange={([minDays, maxDays]) => onChange({ ...hop, minDays, maxDays })}
          />
          <span className="text-[9px] text-[#8b949e] tabular-nums whitespace-nowrap shrink-0">
            {hop.minDays}-{hop.maxDays}d
          </span>
        </div>
      )}
      {isEndpoint && <div className="min-w-[110px]" />}
    </div>
  );
}

// ----- TagInput (include/exclude airport tags) -------------------------

function TagInput({
  placeholder,
  tags,
  color,
  onChange,
}: {
  placeholder: string;
  tags: string[];
  color: string;
  onChange: (next: string[]) => void;
}) {
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);
  const { data: airports = [] } = useAirports();

  const { countryEntries, cityHits } = useMemo(() => {
    if (!focused || !q.trim()) {
      return {
        countryEntries: [] as { name: string; code: string; cities: typeof airports }[],
        cityHits: [] as typeof airports,
      };
    }
    const lq = q.trim().toLowerCase();

    const countryMap = new Map<string, { name: string; code: string; cities: typeof airports }>();
    airports.forEach((a) => {
      if (
        (a.country || "").toLowerCase().includes(lq) ||
        (a.country_code || "").toLowerCase() === lq
      ) {
        const key = a.country_code;
        if (!countryMap.has(key)) countryMap.set(key, { name: a.country || key, code: key, cities: [] });
        if (!tags.includes(a.iata)) countryMap.get(key)!.cities.push(a);
      }
    });

    if (countryMap.size > 0) {
      const entries = [...countryMap.values()].filter((e) => e.cities.length > 0);
      const topCities = entries.flatMap((e) => e.cities).slice(0, 6);
      return { countryEntries: entries, cityHits: topCities };
    }

    const cities = airports
      .filter(
        (a) =>
          !tags.includes(a.iata) &&
          (a.iata.toLowerCase().includes(lq) ||
            (a.city || "").toLowerCase().includes(lq) ||
            (a.name || "").toLowerCase().includes(lq)),
      )
      .slice(0, 6);
    return { countryEntries: [], cityHits: cities };
  }, [focused, q, airports, tags]);

  const hasResults = countryEntries.length > 0 || cityHits.length > 0;

  function addMany(iatas: string[]) {
    // Prepend so newly-added items appear at the left edge of the tag
    // list (right next to the input) rather than getting buried at the
    // far right of an overflow-scrolled row.
    const fresh = iatas.filter((c) => !tags.includes(c));
    onChange([...fresh, ...tags]);
    setQ("");
  }

  return (
    <div className="relative flex-1 min-w-0">
      <div className="flex items-center gap-0.5 bg-[#0d1117] border border-[#21262d] rounded px-1 py-[2px] overflow-x-auto no-scrollbar">
        {/* Input first so there's always a typing area at the left, even
            when tags overflow horizontally. Tags scroll right of it. */}
        <input
          type="text"
          placeholder={tags.length === 0 ? placeholder : ""}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onFocus={() => setFocused(true)}
          onBlur={() => setTimeout(() => setFocused(false), 150)}
          className="flex-1 min-w-[12px] bg-transparent text-[9px] text-[#c9d1d9] outline-none placeholder-[#484f58] py-px"
        />
        {tags.map((t) => (
          <span
            key={t}
            className="shrink-0 inline-flex items-center gap-px px-1 rounded text-[8px] font-semibold"
            style={{ background: `${color}22`, color }}
          >
            {t}
            <button
              onClick={() => onChange(tags.filter((x) => x !== t))}
              className="ml-0.5 text-[7px] opacity-70 hover:opacity-100"
            >
              ×
            </button>
          </span>
        ))}
      </div>
      {hasResults && (
        <div className="absolute top-full left-0 right-0 mt-0.5 bg-[#1c2128] border border-[#30363d] rounded shadow-lg z-[110] max-h-40 overflow-y-auto">
          {countryEntries.map((entry) => (
            <button
              key={`country-${entry.code}`}
              onMouseDown={(e) => {
                e.preventDefault();
                e.stopPropagation();
                addMany(entry.cities.map((a) => a.iata));
              }}
              className="w-full text-left px-2 py-1 text-[9px] hover:bg-[#21262d] font-semibold border-b border-[#30363d] flex items-center gap-1.5"
              style={{ color }}
            >
              <span>{entry.name}</span>
              <span className="text-[#484f58] font-normal ml-auto">{entry.cities.length} cities</span>
            </button>
          ))}
          {cityHits.map((a) => (
            <button
              key={a.iata}
              onMouseDown={(e) => {
                e.preventDefault();
                e.stopPropagation();
                if (!tags.includes(a.iata)) onChange([a.iata, ...tags]);
                setQ("");
              }}
              className="w-full text-left px-2 py-0.5 text-[9px] hover:bg-[#21262d] flex items-center gap-1"
            >
              <span className="font-mono font-bold" style={{ color }}>{a.iata}</span>
              <span className="text-[#8b949e] truncate">{a.city || a.name}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
