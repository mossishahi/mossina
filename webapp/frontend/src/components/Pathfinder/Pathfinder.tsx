import { useState, useMemo, useEffect, useRef } from "react";
import { ChevronRight } from "lucide-react";
import { useSearchPaths, useSearchCycles } from "@/hooks/useSearch";
import { useAirports } from "@/hooks/useAirports";
import { useMapStore } from "@/stores/mapStore";
import { useFilterStore } from "@/stores/filterStore";
import { usePathStore, pathKey_ } from "@/stores/pathStore";
import { useTabStore } from "@/stores/tabStore";
import { AIRLINE_META } from "@/api/types";
import type { PathResult } from "@/api/types";
import SearchControls from "./SearchControls";

export default function Pathfinder() {
  const activeTab = useTabStore((s) => s.activeTab);
  const isCycle = activeTab === "cycles";
  const label = isCycle ? "cycle" : "path";

  const [maxHops, setMaxHops] = useState(3);
  const [openGroupsPath, setOpenGroupsPath] = useState<Set<string>>(new Set());
  const [openGroupsCycle, setOpenGroupsCycle] = useState<Set<string>>(new Set());
  const originCities = useMapStore((s) => s.originCities);
  const destinationCities = useMapStore((s) => s.destinationCities);
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

  const pathMutation = useSearchPaths();
  const cycleMutation = useSearchCycles();

  const prevOriginsRef = useRef(originCities);
  const prevDestsRef = useRef(destinationCities);
  useEffect(() => {
    if (prevOriginsRef.current !== originCities || prevDestsRef.current !== destinationCities) {
      prevOriginsRef.current = originCities;
      prevDestsRef.current = destinationCities;
      clearPaths();
      setSearchActive(false);
      pathMutation.reset();
      cycleMutation.reset();
      setOpenGroupsPath(new Set());
      setOpenGroupsCycle(new Set());
    }
  }, [originCities, destinationCities]);

  function handleSearchBoth() {
    const origins = [...originCities];
    const destinations = [...destinationCities];
    if (origins.length === 0) return;

    const from = dateFrom || new Date().toISOString().slice(0, 10);
    const to =
      dateTo ||
      new Date(Date.now() + 7 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);

    setOpenGroupsPath(new Set());
    setOpenGroupsCycle(new Set());
    clearPaths();
    setSearchActive(true);

    const al = activeAirlines.size === 1 ? [...activeAirlines][0] : undefined;

    pathMutation.mutate({
      origins,
      destinations: destinations.length > 0 ? destinations : origins,
      max_hops: maxHops,
      date_from: from,
      date_to: to,
      only_selected: false,
      airline: al,
    });

    cycleMutation.mutate({
      origins,
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

  function cityName(iata: string) {
    return nameMap.get(iata) || iata;
  }

  function getGroupKey(p: PathResult): string {
    if (isCycle) {
      // Group cycles by their unique intermediate cities (path starts and ends at origin)
      const origin = p.path[0];
      const intermediates = [...new Set(p.path.slice(1, -1).filter((c) => c !== origin))].sort();
      return intermediates.length > 0 ? intermediates.join("+") : (p.path[1] || origin);
    } else {
      // Group paths by which "to" cities appear in the path, excluding "from" cities
      const keyCities = [...destinationCities]
        .filter((d) => !originCities.has(d) && p.path.includes(d))
        .sort();
      // Fallback to terminal city if none of the selected destinations appear
      return keyCities.length > 0 ? keyCities.join("+") : p.path[p.path.length - 1];
    }
  }

  const grouped = useMemo(() => {
    const g: Record<string, PathResult[]> = {};
    results.forEach((p) => {
      const key = getGroupKey(p);
      if (!g[key]) g[key] = [];
      g[key].push(p);
    });
    Object.values(g).forEach((arr) =>
      arr.sort((a, b) => (a.total_cost_eur ?? Infinity) - (b.total_cost_eur ?? Infinity)),
    );
    return g;
  }, [results, isCycle, destinationCities, originCities]);

  // Sort groups by their cheapest path
  const groupKeys = useMemo(
    () =>
      Object.keys(grouped).sort((a, b) => {
        const minA = grouped[a][0]?.total_cost_eur ?? Infinity;
        const minB = grouped[b][0]?.total_cost_eur ?? Infinity;
        return minA - minB;
      }),
    [grouped],
  );

  // Auto-open the cheapest group when results first arrive
  useMemo(() => {
    if (groupKeys.length > 0 && openGroups.size === 0) {
      setOpenGroups(new Set([groupKeys[0]]));
    }
  }, [results]);

  function toggleGroup(key: string) {
    setOpenGroups((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

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
            <div>
              <div className="pb-2">
                <p className="text-xs text-[#484f58]">
                  {results.length} {label}s
                  {searchTimeMs != null && ` in ${(searchTimeMs / 1000).toFixed(1)}s`}
                </p>
              </div>

              {groupKeys.map((key) => {
                const paths = grouped[key];
                const isOpen = openGroups.has(key);
                const minCost = paths[0]?.total_cost_eur;
                const cityCodes = key.split("+");

                return (
                  <div key={key}>
                    <button
                      onClick={() => toggleGroup(key)}
                      className="w-full flex items-center gap-2 px-1 py-1.5 text-left hover:bg-[#161b22]/60 transition-colors border-t border-[#21262d]"
                    >
                      <ChevronRight
                        size={12}
                        className={`text-[#8b949e] transition-transform shrink-0 ${isOpen ? "rotate-90" : ""}`}
                      />
                      <span className="text-sm font-semibold text-[#c9d1d9] truncate flex-1">
                        {cityCodes.map((c) => cityName(c)).join(", ")}
                      </span>
                      {minCost != null && (
                        <span className="text-xs text-[#3fb950] font-medium shrink-0">
                          from {Math.round(minCost)}€
                        </span>
                      )}
                      <span className="text-xs text-[#484f58] bg-[#161b22] px-1.5 py-0.5 rounded-full shrink-0">
                        {paths.length}
                      </span>
                    </button>

                    {isOpen && (
                      <div className="pb-1 space-y-1">
                        {paths.map((p, i) => (
                          <PathCard
                            key={i}
                            result={p}
                            cityName={cityName}
                            pathKey={pathKey_(p)}
                          />
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {mutation.isSuccess && results.length === 0 && (
            <p className="text-xs text-[#8b949e] text-center py-2">
              No {label}s found
            </p>
          )}

          {!mutation.isSuccess && !mutation.isPending && !mutation.isError && (
            <p className="text-xs text-[#484f58] text-center py-2">
              Choose origins and destinations, then Search
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
}: {
  result: PathResult;
  cityName: (iata: string) => string;
  pathKey: string;
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

  // Default to 1-day minimum stay per hop
  const defaultMinDays = result.legs.map(() => 24);

  function handlePriceClick(e: React.MouseEvent) {
    e.stopPropagation();
    if (!isSelected) {
      togglePath(result, activeTab);
      setMinDays(pathKey, defaultMinDays);
    }
    autoSelectBestDates(pathKey, result);
  }

  return (
    <div
      onClick={() => {
        togglePath(result, activeTab);
        if (!isSelected) setMinDays(pathKey, defaultMinDays);
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
          className="text-sm font-bold text-[#3fb950] tabular-nums hover:underline hover:text-[#56d364] cursor-pointer"
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
                className="text-xs text-[#c9d1d9] font-medium whitespace-nowrap"
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
