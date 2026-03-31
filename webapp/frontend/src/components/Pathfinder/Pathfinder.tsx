import { useState } from "react";
import { X, Route, RotateCcw, Loader2, ArrowRight } from "lucide-react";
import { useSearchPaths, useSearchCycles } from "@/hooks/useSearch";
import { useMapStore } from "@/stores/mapStore";
import { useFilterStore } from "@/stores/filterStore";
import { AIRLINE_META } from "@/api/types";
import type { PathResult } from "@/api/types";

type Mode = "path" | "cycle";

interface Props {
  onClose: () => void;
}

export default function Pathfinder({ onClose }: Props) {
  const [mode, setMode] = useState<Mode>("path");
  const [maxHops, setMaxHops] = useState(3);
  const selectedCities = useMapStore((s) => s.selectedCities);
  const activeAirlines = useMapStore((s) => s.activeAirlines);
  const { dateFrom, dateTo } = useFilterStore();

  const pathMutation = useSearchPaths();
  const cycleMutation = useSearchCycles();

  const mutation = mode === "path" ? pathMutation : cycleMutation;
  const results = mutation.data?.paths ?? [];
  const elapsed = mutation.data?.elapsed_seconds;

  function handleSearch() {
    const cities = [...selectedCities];
    if (cities.length === 0) return;

    const from = dateFrom || new Date().toISOString().slice(0, 10);
    const to =
      dateTo ||
      new Date(Date.now() + 7 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);

    if (mode === "path") {
      pathMutation.mutate({
        origins: cities,
        destinations: cities,
        max_hops: maxHops,
        date_from: from,
        date_to: to,
        only_selected: true,
        airline: activeAirlines.size === 1 ? [...activeAirlines][0] : undefined,
      });
    } else {
      cycleMutation.mutate({
        origins: cities,
        max_hops: maxHops,
        date_from: from,
        date_to: to,
        only_selected: true,
      });
    }
  }

  const grouped = results.reduce(
    (acc, p) => {
      const hops = p.legs.length;
      if (!acc[hops]) acc[hops] = [];
      acc[hops].push(p);
      return acc;
    },
    {} as Record<number, PathResult[]>,
  );

  Object.values(grouped).forEach((g) =>
    g.sort((a, b) => a.total_eur - b.total_eur),
  );

  return (
    <div className="absolute top-4 right-4 bottom-4 w-96 bg-black/90 backdrop-blur-xl border border-[#30363d] rounded-xl flex flex-col overflow-hidden z-20">
      <div className="flex items-center justify-between p-4 border-b border-[#30363d]">
        <h2 className="text-sm font-semibold text-white flex items-center gap-2">
          <Route size={16} className="text-[#58a6ff]" />
          Pathfinder
        </h2>
        <button
          onClick={onClose}
          className="text-[#8b949e] hover:text-white transition-colors"
        >
          <X size={16} />
        </button>
      </div>

      <div className="p-4 border-b border-[#30363d] space-y-3">
        <div className="flex gap-2">
          <button
            onClick={() => setMode("path")}
            className={`flex-1 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              mode === "path"
                ? "bg-[#58a6ff]/15 text-[#58a6ff] border border-[#58a6ff]/30"
                : "bg-[#161b22] text-[#8b949e] border border-[#30363d]"
            }`}
          >
            <span className="flex items-center justify-center gap-1.5">
              <ArrowRight size={14} />
              Path
            </span>
          </button>
          <button
            onClick={() => setMode("cycle")}
            className={`flex-1 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              mode === "cycle"
                ? "bg-[#58a6ff]/15 text-[#58a6ff] border border-[#58a6ff]/30"
                : "bg-[#161b22] text-[#8b949e] border border-[#30363d]"
            }`}
          >
            <span className="flex items-center justify-center gap-1.5">
              <RotateCcw size={14} />
              Cycle
            </span>
          </button>
        </div>

        <div className="flex items-center gap-3">
          <label className="text-xs text-[#8b949e]">Max hops</label>
          <input
            type="number"
            min={1}
            max={10}
            value={maxHops}
            onChange={(e) => setMaxHops(Number(e.target.value))}
            className="w-16 bg-[#161b22] border border-[#30363d] rounded-lg py-1 px-2 text-sm text-[#c9d1d9] text-center focus:outline-none focus:border-[#58a6ff]"
          />
          <button
            onClick={handleSearch}
            disabled={mutation.isPending || selectedCities.size === 0}
            className="flex-1 py-1.5 bg-[#238636] hover:bg-[#2ea043] disabled:opacity-50 disabled:hover:bg-[#238636] text-white text-sm font-medium rounded-lg transition-colors flex items-center justify-center gap-2"
          >
            {mutation.isPending ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Searching...
              </>
            ) : (
              "Search"
            )}
          </button>
        </div>
        {selectedCities.size === 0 && (
          <p className="text-xs text-[#e5534b]">
            Select cities on the map first
          </p>
        )}
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        {mutation.isError && (
          <div className="text-sm text-[#e5534b] bg-[#e5534b]/10 border border-[#e5534b]/30 rounded-lg p-3">
            Search failed. Please try again.
          </div>
        )}

        {results.length > 0 && (
          <div className="space-y-4">
            {elapsed != null && (
              <p className="text-xs text-[#8b949e]">
                {results.length} results in {elapsed.toFixed(1)}s
              </p>
            )}
            {Object.entries(grouped)
              .sort(([a], [b]) => Number(a) - Number(b))
              .map(([hops, paths]) => (
                <div key={hops}>
                  <h4 className="text-xs font-medium text-[#8b949e] uppercase tracking-wider mb-2">
                    {hops} {Number(hops) === 1 ? "hop" : "hops"}
                  </h4>
                  <div className="space-y-1.5">
                    {paths.slice(0, 20).map((p, i) => (
                      <PathCard key={i} path={p} />
                    ))}
                    {paths.length > 20 && (
                      <p className="text-xs text-[#484f58] text-center py-1">
                        +{paths.length - 20} more
                      </p>
                    )}
                  </div>
                </div>
              ))}
          </div>
        )}

        {mutation.isSuccess && results.length === 0 && (
          <p className="text-sm text-[#8b949e] text-center py-8">
            No paths found for selected criteria
          </p>
        )}
      </div>
    </div>
  );
}

function PathCard({ path }: { path: PathResult }) {
  return (
    <div className="bg-[#161b22] border border-[#30363d] rounded-lg p-2.5">
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-sm font-semibold text-[#3fb950]">
          EUR {path.total_eur.toFixed(2)}
        </span>
        <span className="text-xs text-[#484f58]">
          {path.legs.length} {path.legs.length === 1 ? "leg" : "legs"}
        </span>
      </div>
      <div className="flex items-center gap-1 flex-wrap">
        {path.cities.map((city, i) => {
          const leg = i < path.legs.length ? path.legs[i] : null;
          const color = leg
            ? AIRLINE_META[leg.airline]?.color || "#58a6ff"
            : "#58a6ff";
          return (
            <span key={i} className="flex items-center gap-1">
              <span className="text-xs font-mono text-[#c9d1d9] font-medium">
                {city}
              </span>
              {i < path.cities.length - 1 && (
                <ArrowRight size={10} style={{ color }} />
              )}
            </span>
          );
        })}
      </div>
    </div>
  );
}
