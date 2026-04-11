import { useMemo } from "react";
import { Loader2, Search, Compass, AlertTriangle } from "lucide-react";
import { useMapStore } from "@/stores/mapStore";
import { useAirports } from "@/hooks/useAirports";
import { useRoutes } from "@/hooks/useRoutes";
import { useTabStore } from "@/stores/tabStore";
import CityPicker from "./CityPicker";

interface Props {
  maxHops: number;
  setMaxHops: (v: number) => void;
  onSearch: () => void;
  pathPending: boolean;
  cyclePending: boolean;
  pathDone: boolean;
  cycleDone: boolean;
}

export default function SearchControls({
  maxHops, setMaxHops, onSearch,
  pathPending, cyclePending,
}: Props) {
  const originCities = useMapStore((s) => s.originCities);
  const destinationCities = useMapStore((s) => s.destinationCities);
  const toggleOrigin = useMapStore((s) => s.toggleOrigin);
  const toggleDestination = useMapStore((s) => s.toggleDestination);
  const { data: airports = [] } = useAirports();
  const { data: routes = [] } = useRoutes();
  const activeTab = useTabStore((s) => s.activeTab);
  const isCycle = activeTab === "cycles";
  const anyPending = pathPending || cyclePending;

  const canSearch = isCycle
    ? originCities.size > 0
    : originCities.size > 0 && destinationCities.size > 0;

  // Airports directly reachable from any selected origin
  const reachableFromOrigins = useMemo(() => {
    if (originCities.size === 0) return undefined;
    const reachable = new Set<string>();
    routes.forEach((r) => {
      if (originCities.has(r.origin)) reachable.add(r.destination);
    });
    return reachable;
  }, [routes, originCities]);

  // Name map for warning labels
  const nameMap = useMemo(() => {
    const m = new Map<string, string>();
    airports.forEach((a) => m.set(a.iata, a.city || a.name || a.iata));
    return m;
  }, [airports]);

  // Selected destinations that have no direct flight from any origin
  const unreachableDests = useMemo(() => {
    if (!reachableFromOrigins || destinationCities.size === 0) return [];
    return [...destinationCities].filter((iata) => !reachableFromOrigins.has(iata));
  }, [reachableFromOrigins, destinationCities]);

  return (
    <div className="bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden">
      <div className="flex items-center gap-2 px-4 pt-3 pb-1.5">
        <Compass size={15} className="text-[#58a6ff] shrink-0" />
        <span className="text-sm font-semibold text-white flex-1">Route Finder</span>
      </div>

      <div className="px-4 pb-4 space-y-3">
        {/* From */}
        <div className="space-y-1.5">
          <label className="text-xs text-[#8b949e] font-medium flex items-center gap-1.5">
            From
            <span className="text-[#484f58]">· up to 3 cities</span>
          </label>
          <CityPicker
            selected={originCities}
            onToggle={toggleOrigin}
            airports={airports}
            placeholder="Search cities…"
            maxSelect={3}
          />
        </div>

        {/* To — hidden for cycles */}
        {!isCycle && (
          <div className="space-y-1.5">
            <label className="text-xs text-[#8b949e] font-medium">To</label>
            <CityPicker
              selected={destinationCities}
              onToggle={toggleDestination}
              airports={airports}
              placeholder="Search cities…"
              reachable={reachableFromOrigins}
            />
            {unreachableDests.length > 0 && (
              <div className="flex items-start gap-1.5 text-xs text-[#9e6a03] bg-[#9e6a03]/10 border border-[#9e6a03]/30 rounded-md px-2.5 py-2">
                <AlertTriangle size={12} className="shrink-0 mt-0.5" />
                <span>
                  No direct flights to{" "}
                  {unreachableDests.map((iata) => nameMap.get(iata) || iata).join(", ")}
                  {" "}from selected origins
                </span>
              </div>
            )}
          </div>
        )}

        {/* Max hops + search button */}
        <div className="flex items-center gap-3 pt-1">
          <span className="text-xs text-[#8b949e] shrink-0">Max hops</span>
          <input
            type="number"
            min={1}
            max={10}
            value={maxHops}
            onChange={(e) => setMaxHops(Number(e.target.value))}
            className="w-14 bg-[#0d1117] border border-[#30363d] rounded-md py-1.5 px-2 text-sm text-[#c9d1d9] text-center focus:outline-none focus:border-[#58a6ff] transition-colors"
          />
          <button
            onClick={onSearch}
            disabled={anyPending || !canSearch}
            className="flex-1 py-2 bg-[#238636] hover:bg-[#2ea043] disabled:opacity-40 text-white text-sm font-semibold rounded-md transition-all flex items-center justify-center gap-2 shadow-[0_1px_3px_rgba(0,0,0,0.3)]"
          >
            {anyPending ? (
              <Loader2 size={14} className="animate-spin" />
            ) : (
              <Search size={14} />
            )}
            {anyPending ? "Searching…" : "Search"}
          </button>
        </div>
      </div>
    </div>
  );
}
