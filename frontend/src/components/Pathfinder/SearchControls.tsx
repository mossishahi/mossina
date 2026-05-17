import { Loader2, Search, Compass } from "lucide-react";
import { useMapStore } from "@/stores/mapStore";

interface Props {
  maxStops: number;
  setMaxStops: (v: number) => void;
  onSearch: () => void;
  pathPending: boolean;
  cyclePending: boolean;
  pathDone: boolean;
  cycleDone: boolean;
}

export default function SearchControls({
  maxStops, setMaxStops, onSearch,
  pathPending, cyclePending,
}: Props) {
  const selectedCities = useMapStore((s) => s.selectedCities);
  const anyPending = pathPending || cyclePending;

  return (
    <div className="bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden pointer-events-auto">
      <div className="flex items-center gap-2 px-3 pt-2.5 pb-1">
        <Compass size={13} className="text-[#58a6ff] shrink-0" />
        <span className="text-xs font-semibold text-white flex-1">Route Finder</span>
      </div>

      <div className="flex items-center gap-2 px-3 pb-2.5">
        <span className="text-[11px] text-[#8b949e] shrink-0">Max stops</span>
        <input
          type="number"
          min={2}
          max={10}
          value={maxStops}
          onChange={(e) => setMaxStops(Number(e.target.value))}
          className="w-12 bg-[#0d1117] border border-[#30363d] rounded-md py-1 px-1.5 text-xs text-[#c9d1d9] text-center focus:outline-none focus:border-[#58a6ff] transition-colors"
        />
        <button
          onClick={onSearch}
          disabled={anyPending || selectedCities.size === 0}
          className="flex-1 py-1.5 bg-[#238636] hover:bg-[#2ea043] disabled:opacity-40 text-white text-xs font-semibold rounded-md transition-all flex items-center justify-center gap-1.5 shadow-[0_1px_3px_rgba(0,0,0,0.3)]"
        >
          {anyPending ? (
            <Loader2 size={12} className="animate-spin" />
          ) : (
            <Search size={12} />
          )}
          {anyPending ? "Searching\u2026" : "Search"}
        </button>
      </div>
    </div>
  );
}
