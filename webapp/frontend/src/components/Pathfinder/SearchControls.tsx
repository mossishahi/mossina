import { Loader2, Search, Compass } from "lucide-react";
import { useMapStore } from "@/stores/mapStore";
import { useAirports } from "@/hooks/useAirports";
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
  const activeTab = useTabStore((s) => s.activeTab);
  const isCycle = activeTab === "cycles";
  const anyPending = pathPending || cyclePending;

  const canSearch = isCycle
    ? originCities.size > 0
    : originCities.size > 0 && destinationCities.size > 0;

  return (
    <div className="bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden">
      <div className="flex items-center gap-2 px-3 pt-2.5 pb-1">
        <Compass size={13} className="text-[#58a6ff] shrink-0" />
        <span className="text-xs font-semibold text-white flex-1">Route Finder</span>
      </div>

      <div className="px-3 pb-2.5 space-y-2">
        {/* From */}
        <div className="space-y-1">
          <label className="text-[10px] text-[#8b949e] font-medium">
            From
            <span className="text-[#484f58] ml-1">up to 3</span>
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
          <div className="space-y-1">
            <label className="text-[10px] text-[#8b949e] font-medium">To</label>
            <CityPicker
              selected={destinationCities}
              onToggle={toggleDestination}
              airports={airports}
              placeholder="Search cities…"
            />
          </div>
        )}

        {/* Max hops + search button */}
        <div className="flex items-center gap-2 pt-0.5">
          <span className="text-[11px] text-[#8b949e] shrink-0">Max hops</span>
          <input
            type="number"
            min={1}
            max={10}
            value={maxHops}
            onChange={(e) => setMaxHops(Number(e.target.value))}
            className="w-12 bg-[#0d1117] border border-[#30363d] rounded-md py-1 px-1.5 text-xs text-[#c9d1d9] text-center focus:outline-none focus:border-[#58a6ff] transition-colors"
          />
          <button
            onClick={onSearch}
            disabled={anyPending || !canSearch}
            className="flex-1 py-1.5 bg-[#238636] hover:bg-[#2ea043] disabled:opacity-40 text-white text-xs font-semibold rounded-md transition-all flex items-center justify-center gap-1.5 shadow-[0_1px_3px_rgba(0,0,0,0.3)]"
          >
            {anyPending ? (
              <Loader2 size={12} className="animate-spin" />
            ) : (
              <Search size={12} />
            )}
            {anyPending ? "Searching…" : "Search"}
          </button>
        </div>
      </div>
    </div>
  );
}
