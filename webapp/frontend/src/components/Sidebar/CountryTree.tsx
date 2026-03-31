import { useState, useMemo } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useAirports } from "@/hooks/useAirports";
import { useMapStore } from "@/stores/mapStore";
import clsx from "clsx";

const INITIAL_VISIBLE = 10;

export default function CountryTree() {
  const { data: airports = [] } = useAirports();
  const selectedCities = useMapStore((s) => s.selectedCities);
  const toggleCity = useMapStore((s) => s.toggleCity);
  const toggleCountry = useMapStore((s) => s.toggleCountry);
  const clearSelection = useMapStore((s) => s.clearSelection);
  const setSelectedCities = useMapStore((s) => s.setSelectedCities);

  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [showAll, setShowAll] = useState(false);

  const grouped = useMemo(() => {
    const map = new Map<string, { name: string; airports: typeof airports }>();
    airports.forEach((a) => {
      if (!map.has(a.country)) {
        map.set(a.country, { name: a.country, airports: [] });
      }
      map.get(a.country)!.airports.push(a);
    });
    const entries = [...map.entries()];
    entries.sort(([, a], [, b]) => {
      const aSelected = a.airports.some((ap) => selectedCities.has(ap.iata));
      const bSelected = b.airports.some((ap) => selectedCities.has(ap.iata));
      if (aSelected && !bSelected) return -1;
      if (!aSelected && bSelected) return 1;
      return a.name.localeCompare(b.name);
    });
    return entries;
  }, [airports, selectedCities]);

  const expandedWithSelected = useMemo(() => {
    const s = new Set(expanded);
    grouped.forEach(([country, data]) => {
      if (data.airports.some((a) => selectedCities.has(a.iata))) {
        s.add(country);
      }
    });
    return s;
  }, [expanded, grouped, selectedCities]);

  const allSelected =
    airports.length > 0 && airports.every((a) => selectedCities.has(a.iata));

  const visible = showAll ? grouped : grouped.slice(0, INITIAL_VISIBLE);

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-xs font-medium text-[#8b949e] uppercase tracking-wider">
          Countries
        </h3>
        <label className="flex items-center gap-1.5 text-xs text-[#8b949e] cursor-pointer">
          <input
            type="checkbox"
            checked={allSelected}
            onChange={() => {
              if (allSelected) {
                clearSelection();
              } else {
                setSelectedCities(airports.map((a) => a.iata));
              }
            }}
            className="accent-[#58a6ff] w-3 h-3"
          />
          All
        </label>
      </div>

      <div className="space-y-0.5 max-h-64 overflow-y-auto">
        {visible.map(([country, data]) => {
          const isExpanded = expandedWithSelected.has(country);
          const codes = data.airports.map((a) => a.iata);
          const countrySelected = codes.every((c) => selectedCities.has(c));
          const countryPartial =
            !countrySelected && codes.some((c) => selectedCities.has(c));

          return (
            <div key={country}>
              <div className="flex items-center gap-1 py-1 px-1 rounded hover:bg-[#161b22] group">
                <button
                  onClick={() => {
                    const next = new Set(expanded);
                    if (next.has(country)) {
                      next.delete(country);
                    } else {
                      next.add(country);
                    }
                    setExpanded(next);
                  }}
                  className="text-[#8b949e] hover:text-[#c9d1d9] p-0.5"
                >
                  {isExpanded ? (
                    <ChevronDown size={12} />
                  ) : (
                    <ChevronRight size={12} />
                  )}
                </button>
                <input
                  type="checkbox"
                  checked={countrySelected}
                  ref={(el) => {
                    if (el) el.indeterminate = countryPartial;
                  }}
                  onChange={() => toggleCountry(codes)}
                  className="accent-[#58a6ff] w-3 h-3"
                />
                <span className="text-sm text-[#c9d1d9] flex-1 truncate">
                  {data.name}
                </span>
                <span className="text-xs text-[#484f58]">
                  {data.airports.length}
                </span>
              </div>

              {isExpanded && (
                <div className="ml-6 space-y-0.5">
                  {data.airports.map((a) => (
                    <label
                      key={a.iata}
                      className={clsx(
                        "flex items-center gap-2 py-0.5 px-1 rounded text-sm cursor-pointer hover:bg-[#161b22]",
                        selectedCities.has(a.iata)
                          ? "text-[#58a6ff]"
                          : "text-[#8b949e]",
                      )}
                    >
                      <input
                        type="checkbox"
                        checked={selectedCities.has(a.iata)}
                        onChange={() => toggleCity(a.iata)}
                        className="accent-[#58a6ff] w-3 h-3"
                      />
                      <span className="font-mono text-xs w-8">{a.iata}</span>
                      <span className="truncate">{a.city || a.name}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {grouped.length > INITIAL_VISIBLE && (
        <button
          onClick={() => setShowAll(!showAll)}
          className="mt-2 text-xs text-[#58a6ff] hover:underline"
        >
          {showAll
            ? "Show less"
            : `Show ${grouped.length - INITIAL_VISIBLE} more countries`}
        </button>
      )}
    </div>
  );
}
