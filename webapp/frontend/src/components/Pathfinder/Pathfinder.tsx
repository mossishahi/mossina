import { useState, useMemo, useCallback, useEffect, useRef } from "react";
import { ChevronRight } from "lucide-react";
import { useSearchPaths, useSearchCycles } from "@/hooks/useSearch";
import { useAirports } from "@/hooks/useAirports";
import { useMapStore } from "@/stores/mapStore";
import { useFilterStore } from "@/stores/filterStore";
import { usePathStore, pathKey_ } from "@/stores/pathStore";
import { useTabStore } from "@/stores/tabStore";
import { AIRLINE_META } from "@/api/types";
import type { PathResult, HopConstraint, LegConstraint } from "@/api/types";
import SearchControls from "./SearchControls";
import HopFilter, { emptyHop, isHopActive } from "./HopFilter";
import type { HopFilterValue } from "./HopFilter";
import LegArrow, { emptyLeg } from "./LegArrow";
import type { LegFilterValue } from "./LegArrow";

export default function Pathfinder() {
  const activeTab = useTabStore((s) => s.activeTab);
  const isCycle = activeTab === "cycles";
  const label = isCycle ? "cycle" : "path";

  const [maxHops, setMaxHops] = useState(3);
  const [onlySelected, setOnlySelected] = useState(false);
  const [openGroupsPath, setOpenGroupsPath] = useState<Set<number>>(new Set());
  const [openGroupsCycle, setOpenGroupsCycle] = useState<Set<number>>(new Set());
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
      setOpenGroupsPath(new Set());
      setOpenGroupsCycle(new Set());
    }
  }, [selectedCities]);

  function handleSearchBoth() {
    const cities = [...selectedCities];
    if (cities.length === 0) return;

    const from = dateFrom || new Date().toISOString().slice(0, 10);
    const to =
      dateTo ||
      new Date(Date.now() + 7 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);

    setOpenGroupsPath(new Set());
    setOpenGroupsCycle(new Set());
    setHopFilters({});
    setLegFilters({});
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

  const mutation = isCycle ? cycleMutation : pathMutation;
  const results = mutation.data?.results ?? [];
  const searchTimeMs = mutation.data?.search_time_ms;
  const openGroups = isCycle ? openGroupsCycle : openGroupsPath;
  const setOpenGroups = isCycle ? setOpenGroupsCycle : setOpenGroupsPath;

  const pathCount = pathMutation.data?.results?.length ?? 0;
  const cycleCount = cycleMutation.data?.results?.length ?? 0;

  const filtered = useMemo(() => {
    if (!onlySelected || selectedCities.size === 0) return results;
    return results.filter((p) => p.path.every((iata) => selectedCities.has(iata)));
  }, [results, onlySelected, selectedCities]);

  const grouped = useMemo(() => {
    const g: Record<number, PathResult[]> = {};
    filtered.forEach((p) => {
      const n = p.legs.length;
      if (!g[n]) g[n] = [];
      g[n].push(p);
    });
    Object.values(g).forEach((arr) =>
      arr.sort((a, b) => (a.total_cost_eur ?? Infinity) - (b.total_cost_eur ?? Infinity)),
    );
    return g;
  }, [filtered]);

  const lengths = Object.keys(grouped).map(Number).sort((a, b) => a - b);

  useMemo(() => {
    if (lengths.length > 0 && openGroups.size === 0) {
      setOpenGroups(new Set([lengths[0]]));
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
  const repricingTimer = useRef<ReturnType<typeof setTimeout>>();

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
          const mutation = isCycle ? cycleMutation : pathMutation;
          const base = isCycle
            ? { origins: s.cities, max_hops: maxHops, date_from: s.from, date_to: s.to, only_selected: false }
            : { origins: s.cities, destinations: s.cities, max_hops: maxHops, date_from: s.from, date_to: s.to, only_selected: false, airline: s.airline };
          mutation.mutate(
            { ...base, hop_filters: hopConstraints } as any,
            {
              onSuccess: (data) => {
                if (data?.results) {
                  updateSelectedResults(data.results, isCycle ? "cycles" : "paths");
                }
              },
              onSettled: () => setRepricing(false),
            },
          );
        }, 600);
      }

      return { ...prev, [length]: next };
    });
  }, [isCycle, maxHops]);

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
        onSearch={handleSearchBoth}
        pathPending={pathMutation.isPending}
        cyclePending={cycleMutation.isPending}
        pathDone={pathMutation.isSuccess}
        cycleDone={cycleMutation.isSuccess}
      />

      <div className="bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden">
        <TabBarInline
          pathCount={pathCount}
          cycleCount={cycleCount}
          pathPending={pathMutation.isPending}
          cyclePending={cycleMutation.isPending}
        />

        <div className="px-3 py-2 space-y-1.5">
          {mutation.isError && (
            <div className="text-xs text-[#e5534b] bg-[#e5534b]/10 border border-[#e5534b]/30 rounded-md p-2.5">
              Search failed. Please try again.
            </div>
          )}

          {mutation.isPending && (
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
                  {filtered.length}
                  {onlySelected && filtered.length !== results.length
                    ? ` of ${results.length}`
                    : ""}{" "}
                  {label}s
                  {searchTimeMs != null && ` in ${(searchTimeMs / 1000).toFixed(1)}s`}
                </p>
                <label className="flex items-center gap-2 cursor-pointer select-none">
                  <input
                    type="checkbox"
                    checked={onlySelected}
                    onChange={(e) => setOnlySelected(e.target.checked)}
                    className="accent-[#58a6ff] w-3 h-3 rounded"
                  />
                  <span className="text-[11px] text-[#8b949e]">
                    Only selected cities
                  </span>
                </label>
              </div>

              {lengths.map((n) => {
                const allPaths = grouped[n];
                const hopFilteredPaths = applyHopFilters(allPaths, n);
                const isOpen = openGroups.has(n);
                const hf = getHopFiltersForLength(n);
                return (
                  <div key={n}>
                    <button
                      onClick={() => toggleGroup(n)}
                      className="w-full flex items-center gap-2 px-1 py-1.5 text-left hover:bg-[#161b22]/60 transition-colors border-t border-[#21262d]"
                    >
                      <ChevronRight
                        size={12}
                        className={`text-[#8b949e] transition-transform ${isOpen ? "rotate-90" : ""}`}
                      />
                      <span className="text-xs font-semibold text-[#58a6ff]">
                        Length {n}
                      </span>
                      <span className="text-[10px] text-[#484f58] bg-[#161b22] px-1.5 py-0.5 rounded-full">
                        {hopFilteredPaths.length !== allPaths.length
                          ? `${hopFilteredPaths.length}/${allPaths.length}`
                          : `${allPaths.length}`}
                      </span>
                    </button>

                    {isOpen && (
                      <div className="pb-1">
                        <div className="flex gap-0.5 px-2 py-2 items-center justify-center">
                          {Array.from({ length: n + 1 }, (_, i) => {
                            const lf = getLegFiltersForLength(n);
                            return (
                              <span key={i} className="inline-flex items-center">
                                <HopFilter
                                  index={i}
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
                            <PathCard key={i} result={p} cityName={cityName} pathKey={pathKey_(p)} hopFilters={hf} legFilters={getLegFiltersForLength(n)} />
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

          {mutation.isSuccess && filtered.length === 0 && (
            <p className="text-xs text-[#8b949e] text-center py-2">
              {results.length > 0 && onlySelected
                ? "No results match the selected cities filter"
                : `No ${label}s found`}
            </p>
          )}

          {!mutation.isSuccess && !mutation.isPending && !mutation.isError && (
            <p className="text-xs text-[#484f58] text-center py-2">
              Select cities and press Search
            </p>
          )}
        </div>
      </div>
    </>
  );
}

function TabBarInline({
  pathCount, cycleCount, pathPending, cyclePending,
}: {
  pathCount: number; cycleCount: number;
  pathPending: boolean; cyclePending: boolean;
}) {
  const activeTab = useTabStore((s) => s.activeTab);
  const setActiveTab = useTabStore((s) => s.setActiveTab);

  const tabs = [
    { id: "paths" as const, label: "Paths", count: pathCount, pending: pathPending },
    { id: "cycles" as const, label: "Cycles", count: cycleCount, pending: cyclePending },
  ];

  return (
    <div className="bg-[#0d1117]" style={{ borderTopLeftRadius: "10px", borderTopRightRadius: "10px" }}>
      <div className="flex items-end px-2 pt-2 gap-0.5">
        {tabs.map((tab) => {
          const active = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`relative flex items-center gap-1.5 px-4 py-1.5 text-xs font-semibold transition-colors ${
                active
                  ? "text-[#58a6ff] bg-black/80 border border-[#30363d] border-b-transparent -mb-px"
                  : "text-[#484f58] hover:text-[#8b949e] bg-transparent border border-transparent"
              }`}
              style={{
                borderTopLeftRadius: "8px",
                borderTopRightRadius: "8px",
                borderBottomLeftRadius: "0",
                borderBottomRightRadius: "0",
              }}
            >
              {tab.label}
              {tab.count > 0 && (
                <span className={`text-[9px] px-1 py-0.5 rounded-full ${
                  active ? "bg-[#3fb950]/15 text-[#3fb950]" : "bg-[#21262d] text-[#484f58]"
                }`}>
                  {tab.count}
                </span>
              )}
            </button>
          );
        })}
      </div>
      <div className="relative h-[2px] bg-[#30363d] overflow-hidden">
        {(tabs.some((t) => t.pending)) && (
          <div className="absolute inset-0 shimmer-line" />
        )}
      </div>
    </div>
  );
}

function PathCard({
  result,
  cityName,
  pathKey,
  hopFilters,
  legFilters,
}: {
  result: PathResult;
  cityName: (iata: string) => string;
  pathKey: string;
  hopFilters: HopFilterValue[];
  legFilters: LegFilterValue[];
}) {
  const activeTab = useTabStore((s) => s.activeTab);
  const selectedPaths = usePathStore((s) => s.selectedPaths);
  const togglePath = usePathStore((s) => s.togglePath);
  const setMinDays = usePathStore((s) => s.setMinDays);
  const autoSelectBestDates = usePathStore((s) => s.autoSelectBestDates);
  const isSelected = selectedPaths.some(
    (tp) => pathKey_(tp.result) === pathKey && tp.tab === activeTab,
  );

  const cost = result.total_cost_eur;
  const costLabel =
    cost != null
      ? `${result.is_partial ? "~" : ""}${Math.round(cost)}\u20AC`
      : "--";

  function handlePriceClick(e: React.MouseEvent) {
    e.stopPropagation();
    if (!isSelected) {
      togglePath(result, activeTab);
      setMinDays(pathKey, hopFilters.map((hf) => (hf.minDays ?? 1) * 24));
    }
    autoSelectBestDates(pathKey, result);
  }

  return (
    <div
      onClick={() => {
        togglePath(result, activeTab);
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
