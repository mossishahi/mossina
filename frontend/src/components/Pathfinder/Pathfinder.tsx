import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import { ChevronRight, Filter, Check } from "lucide-react";
import { useSearchPaths, useSearchCycles } from "@/hooks/useSearch";
import { useAirports } from "@/hooks/useAirports";
import { useMapStore } from "@/stores/mapStore";
import { useFilterStore } from "@/stores/filterStore";
import { usePathStore, pathKey_ } from "@/stores/pathStore";
import { AIRLINE_META } from "@/api/types";
import type { PathResult } from "@/api/types";
import SearchControls from "./SearchControls";
import HopFilter, { emptyHop } from "./HopFilter";
import type { HopFilterValue } from "./HopFilter";
import LegArrow, { emptyLeg } from "./LegArrow";
import type { LegFilterValue } from "./LegArrow";

// A "round trip" (cycle) is just a path whose origin equals its destination.
function isRoundTrip(p: PathResult): boolean {
  return p.path.length > 1 && p.path[0] === p.path[p.path.length - 1];
}

// Per-group filters. Each "Stops" group has its own copy.
interface GroupFilters {
  onlySelected: boolean;
  onlyRoundTrips: boolean;
}

const emptyGroupFilters = (): GroupFilters => ({
  onlySelected: false,
  onlyRoundTrips: false,
});

function isGroupFilterActive(f: GroupFilters): boolean {
  return f.onlySelected || f.onlyRoundTrips;
}

export default function Pathfinder() {
  const [maxHops, setMaxHops] = useState(3);
  const [groupFilters, setGroupFilters] = useState<Record<number, GroupFilters>>({});
  const [openGroups, setOpenGroups] = useState<Set<number>>(new Set());
  const [hopFilters, setHopFilters] = useState<Record<number, HopFilterValue[]>>({});
  const [legFilters, setLegFilters] = useState<Record<number, LegFilterValue[]>>({});
  const selectedCities = useMapStore((s) => s.selectedCities);
  const activeAirlines = useMapStore((s) => s.activeAirlines);
  const { dateFrom, dateTo } = useFilterStore();

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
      setHopFilters({});
      setLegFilters({});
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
      new Date(Date.now() + 7 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);

    setOpenGroups(new Set());
    setHopFilters({});
    setLegFilters({});
    setGroupFilters({});
    clearPaths();
    setSearchActive(true);

    const al = activeAirlines.size === 1 ? [...activeAirlines][0] : undefined;
    lastSearchRef.current = { from, to, cities, airline: al };

    pathMutation.mutate({
      origins: cities,
      destinations: cities,
      max_hops: maxHops,
      date_from: from,
      date_to: to,
      only_selected: false,
      airline: al,
    });

    cycleMutation.mutate({
      origins: cities,
      max_hops: maxHops,
      date_from: from,
      date_to: to,
      only_selected: false,
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

  // Merge path and cycle search results into a single ordered list. Cycle
  // results have origin == destination; path results never do, so the two
  // sets are disjoint and we don't need to dedup.
  const results = useMemo(() => {
    return [...pathResults, ...cycleResults];
  }, [pathResults, cycleResults]);

  const anyPending = pathMutation.isPending || cycleMutation.isPending;
  const anySuccess = pathMutation.isSuccess || cycleMutation.isSuccess;
  const anyError = pathMutation.isError && cycleMutation.isError;

  // Group raw results by number of stops (= legs.length), sorted by cost.
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

  // Apply a single group's filter (onlySelected / onlyRoundTrips) to its paths.
  const applyGroupFilters = useCallback(
    (paths: PathResult[], stops: number): PathResult[] => {
      const f = groupFilters[stops] || emptyGroupFilters();
      let out = paths;
      if (f.onlyRoundTrips) out = out.filter(isRoundTrip);
      if (f.onlySelected && selectedCities.size > 0) {
        out = out.filter((p) => p.path.every((iata) => selectedCities.has(iata)));
      }
      return out;
    },
    [groupFilters, selectedCities],
  );

  // Total visible-trip count after per-group filters (but before hop/leg filters,
  // which only shrink the expanded view).
  const totalFiltered = useMemo(
    () => stopGroups.reduce((sum, n) => sum + applyGroupFilters(grouped[n] || [], n).length, 0),
    [stopGroups, grouped, applyGroupFilters],
  );

  const anyGroupFilterActive = useMemo(
    () => Object.values(groupFilters).some(isGroupFilterActive),
    [groupFilters],
  );

  // Auto-expand the first stops group when a new result set comes in.
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

  function getHopFiltersForLength(n: number): HopFilterValue[] {
    if (hopFilters[n]) return hopFilters[n];
    return Array.from({ length: n + 1 }, () => emptyHop());
  }

  function getLegFiltersForLength(n: number): LegFilterValue[] {
    if (legFilters[n]) return legFilters[n];
    return Array.from({ length: n }, () => emptyLeg());
  }

  const [repricing, setRepricing] = useState(false);
  const repricingTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const updateHopFilter = useCallback((length: number, stopIdx: number, val: HopFilterValue) => {
    setHopFilters((prev) => {
      const current = prev[length] || Array.from({ length: length + 1 }, () => emptyHop());
      const next = [...current];
      next[stopIdx] = val;

      const hasDays = next.some((h) => h.minDays != null || h.maxDays != null);
      if (hasDays && lastSearchRef.current) {
        clearTimeout(repricingTimer.current);
        setRepricing(true);
        const hopConstraints = next.map((h) => ({
          min_stay_days: h.minDays,
          max_stay_days: h.maxDays,
          include_cities: h.includeCities.length > 0 ? h.includeCities : null,
          exclude_cities: h.excludeCities.length > 0 ? h.excludeCities : null,
        }));
        repricingTimer.current = setTimeout(() => {
          const s = lastSearchRef.current!;
          // Re-run BOTH paths and cycles so the merged list stays in sync.
          pathMutation.mutate(
            {
              origins: s.cities,
              destinations: s.cities,
              max_hops: maxHops,
              date_from: s.from,
              date_to: s.to,
              only_selected: false,
              airline: s.airline,
              hop_filters: hopConstraints,
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
              max_hops: maxHops,
              date_from: s.from,
              date_to: s.to,
              only_selected: false,
              hop_filters: hopConstraints,
            } as any,
            {
              onSuccess: (data) => {
                if (data?.results) updateSelectedResults(data.results);
              },
              onSettled: () => setRepricing(false),
            },
          );
        }, 600);
      }

      return { ...prev, [length]: next };
    });
  }, [maxHops]);

  const updateLegFilter = useCallback((length: number, legIdx: number, val: LegFilterValue) => {
    setLegFilters((prev) => {
      const current = prev[length] || Array.from({ length: length }, () => emptyLeg());
      const next = [...current];
      next[legIdx] = val;
      return { ...prev, [length]: next };
    });
  }, []);

  function applyHopFilters(paths: PathResult[], length: number): PathResult[] {
    const hops = getHopFiltersForLength(length);
    const legs = getLegFiltersForLength(length);
    return paths.filter((p) => {
      for (let i = 0; i < p.path.length; i++) {
        const f = hops[i];
        if (!f) continue;
        if (f.includeCities.length > 0 && !f.includeCities.includes(p.path[i])) return false;
        if (f.excludeCities.includes(p.path[i])) return false;
      }
      for (let i = 0; i < p.legs.length; i++) {
        const lf = legs[i];
        if (!lf) continue;
        if (lf.airline && p.legs[i].airline !== lf.airline) return false;
      }
      return true;
    });
  }

  const allAirlines = useMemo(() => {
    const codes = new Set<string>();
    results.forEach((r) => r.legs.forEach((l) => codes.add(l.airline)));
    return [...codes].sort();
  }, [results]);

  return (
    <>
      <SearchControls
        maxHops={maxHops}
        setMaxHops={setMaxHops}
        onSearch={handleSearch}
        pathPending={pathMutation.isPending}
        cyclePending={cycleMutation.isPending}
        pathDone={pathMutation.isSuccess}
        cycleDone={cycleMutation.isSuccess}
      />

      <div className="bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden">
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
                const hopFilteredPaths = applyHopFilters(groupFiltered, n);
                const isOpen = openGroups.has(n);
                const hf = getHopFiltersForLength(n);
                const filtersValue = groupFilters[n] || emptyGroupFilters();
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
                          {hopFilteredPaths.length !== allPaths.length
                            ? `${hopFilteredPaths.length}/${allPaths.length}`
                            : `${allPaths.length}`}
                        </span>
                      </button>
                      <FilterMenu
                        value={filtersValue}
                        onApply={(next) =>
                          setGroupFilters((prev) => ({ ...prev, [n]: next }))
                        }
                      />
                    </div>

                    {isOpen && (
                      <div className="pb-1">
                        <div className="flex gap-0.5 px-2 py-2 items-center justify-center">
                          {Array.from({ length: n + 1 }, (_, i) => {
                            const lf = getLegFiltersForLength(n);
                            const isEndpoint = i === 0 || i === n;
                            return (
                              <span key={i} className="inline-flex items-center">
                                <HopFilter
                                  index={i}
                                  isEndpoint={isEndpoint}
                                  value={hf[i] || emptyHop()}
                                  onChange={(val) => updateHopFilter(n, i, val)}
                                />
                                {i < n && (
                                  <LegArrow
                                    value={lf[i] || emptyLeg()}
                                    onChange={(val) => updateLegFilter(n, i, val)}
                                    airlines={allAirlines}
                                  />
                                )}
                              </span>
                            );
                          })}
                        </div>
                        <div className="space-y-1">
                          {hopFilteredPaths.map((p, i) => (
                            <PathCard key={i} result={p} cityName={cityName} pathKey={pathKey_(p)} hopFilters={hf} />
                          ))}
                          {hopFilteredPaths.length === 0 && (
                            <p className="text-[10px] text-[#484f58] text-center py-2">No matches</p>
                          )}
                        </div>
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

function PathCard({
  result,
  cityName,
  pathKey,
  hopFilters,
}: {
  result: PathResult;
  cityName: (iata: string) => string;
  pathKey: string;
  hopFilters: HopFilterValue[];
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
      setMinDays(pathKey, hopFilters.map((hf) => (hf.minDays ?? 1) * 24));
    }
    autoSelectBestDates(pathKey, result);
  }

  return (
    <div
      onClick={() => {
        togglePath(result);
        if (!isSelected) {
          setMinDays(pathKey, hopFilters.map((hf) => (hf.minDays ?? 1) * 24));
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
          const color = leg
            ? AIRLINE_META[leg.airline]?.color || "#8b949e"
            : "#8b949e";
          return (
            <span key={i} className="inline-flex items-center gap-0.5">
              <span
                className="text-[10px] text-[#c9d1d9] font-medium whitespace-nowrap"
                title={`${cityName(iata)} (${iata})`}
              >
                {cityName(iata)}
              </span>
              {i < result.path.length - 1 && (
                <svg width="20" height="8" viewBox="0 0 20 8" className="shrink-0 mx-0.5" style={{ color }}>
                  <line x1="0" y1="4" x2="14" y2="4" stroke="currentColor" strokeWidth="1.2" />
                  <path d="M12 1 L17 4 L12 7" stroke="currentColor" strokeWidth="1.2" fill="none" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              )}
            </span>
          );
        })}
      </div>
    </div>
  );
}

function FilterMenu({
  value,
  onApply,
}: {
  value: GroupFilters;
  onApply: (next: GroupFilters) => void;
}) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<GroupFilters>(value);
  const ref = useRef<HTMLDivElement>(null);
  const active = isGroupFilterActive(value);

  function openMenu(e: React.MouseEvent) {
    e.stopPropagation();
    setDraft(value);
    setOpen(true);
  }

  function apply() {
    onApply(draft);
    setOpen(false);
  }

  useEffect(() => {
    if (!open) return;
    function onDocClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, [open]);

  return (
    <div ref={ref} className="relative shrink-0">
      <button
        onClick={openMenu}
        className={`p-1 rounded transition-colors ${
          active
            ? "text-[#58a6ff] hover:text-[#79c0ff]"
            : "text-[#484f58] hover:text-[#8b949e]"
        }`}
        title="Filter this group"
        aria-label="Filter this group"
      >
        <Filter size={12} strokeWidth={2.2} />
      </button>

      {open && (
        <div
          className="absolute right-0 top-full mt-1 z-50 bg-[#161b22] border border-[#30363d] rounded-lg shadow-xl w-60 p-2"
          onClick={(e) => e.stopPropagation()}
        >
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
          <div className="flex justify-end pt-1 mt-1 border-t border-[#21262d]">
            <button
              onClick={apply}
              className="px-2 py-1 text-[10px] font-semibold text-[#3fb950] hover:text-[#56d364] inline-flex items-center gap-1"
              title="Apply filters"
            >
              <Check size={12} />
              Apply
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
