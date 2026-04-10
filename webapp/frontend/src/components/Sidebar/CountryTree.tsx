import { useState, useMemo } from "react";
import { ChevronDown, ChevronRight, Search } from "lucide-react";
import { useAirports } from "@/hooks/useAirports";
import { useMapStore } from "@/stores/mapStore";
import clsx from "clsx";

function countryFlag(code: string): string {
  const cc = code.toUpperCase();
  if (cc.length !== 2) return "";
  return String.fromCodePoint(
    ...cc.split("").map((c) => 0x1f1e6 + c.charCodeAt(0) - 65),
  );
}

export default function CountryTree() {
  const { data: airports = [] } = useAirports();
  const selectedCities = useMapStore((s) => s.selectedCities);
  const toggleCity = useMapStore((s) => s.toggleCity);
  const toggleCountry = useMapStore((s) => s.toggleCountry);
  const clearSelection = useMapStore((s) => s.clearSelection);
  const setSelectedCities = useMapStore((s) => s.setSelectedCities);

  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [query, setQuery] = useState("");

  const grouped = useMemo(() => {
    const map = new Map<string, { name: string; code: string; airports: typeof airports }>();
    airports.forEach((a) => {
      if (!map.has(a.country)) {
        map.set(a.country, { name: a.country, code: a.country_code || a.country, airports: [] });
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

  const filtered = useMemo(() => {
    if (!query.trim()) return grouped;
    const q = query.trim().toLowerCase();
    return grouped
      .map(([country, data]) => {
        const countryMatch = data.name.toLowerCase().includes(q);
        if (countryMatch) return [country, data] as typeof grouped[0];
        const matchedAirports = data.airports.filter(
          (a) =>
            a.iata.toLowerCase().includes(q) ||
            (a.city || "").toLowerCase().includes(q) ||
            (a.name || "").toLowerCase().includes(q),
        );
        if (matchedAirports.length === 0) return null;
        return [country, { ...data, airports: matchedAirports }] as typeof grouped[0];
      })
      .filter(Boolean) as typeof grouped;
  }, [grouped, query]);

  const expandedWithSelected = useMemo(() => {
    const s = new Set(expanded);
    grouped.forEach(([country, data]) => {
      if (data.airports.some((a) => selectedCities.has(a.iata))) {
        s.add(country);
      }
    });
    if (query.trim()) {
      filtered.forEach(([country]) => s.add(country));
    }
    return s;
  }, [expanded, grouped, filtered, selectedCities, query]);

  const allSelected =
    airports.length > 0 && airports.every((a) => selectedCities.has(a.iata));

  const visible = filtered;

  return (
    <div>
      <div className="flex items-center gap-2 mb-2">
        <span className="text-xs font-medium text-[#8b949e] uppercase tracking-wider shrink-0">
          Countries / Cities
        </span>
        <div className="relative flex-1">
          <Search size={10} className="absolute left-2 top-1/2 -translate-y-1/2 text-[#484f58]" />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Filter..."
            className="w-full bg-[#0d1117] border border-[#30363d] rounded-md py-1 pl-6 pr-2 text-[11px] text-[#c9d1d9] placeholder-[#484f58] focus:outline-none focus:border-[#58a6ff] transition-colors"
          />
        </div>
        <label className="flex items-center gap-1 text-[10px] text-[#484f58] cursor-pointer shrink-0">
          <input
            type="checkbox"
            checked={allSelected}
            onChange={() => {
              if (allSelected) clearSelection();
              else setSelectedCities(airports.map((a) => a.iata));
            }}
            className="accent-[#58a6ff] w-2.5 h-2.5"
          />
          All
        </label>
      </div>

      <div className="space-y-px">
        {visible.map(([country, data]) => {
          const isExpanded = expandedWithSelected.has(country);
          const codes = data.airports.map((a) => a.iata);
          const countrySelected = codes.every((c) => selectedCities.has(c));
          const countryPartial =
            !countrySelected && codes.some((c) => selectedCities.has(c));

          return (
            <div key={country}>
              <div className="flex items-center gap-1 py-0.5 px-1 rounded hover:bg-[#161b22] group">
                <button
                  onClick={() => {
                    const next = new Set(expanded);
                    if (next.has(country)) next.delete(country);
                    else next.add(country);
                    setExpanded(next);
                  }}
                  className="text-[#8b949e] hover:text-[#c9d1d9] p-0.5"
                >
                  {isExpanded ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                </button>
                <input
                  type="checkbox"
                  checked={countrySelected}
                  ref={(el) => { if (el) el.indeterminate = countryPartial; }}
                  onChange={() => { toggleCountry(codes); setQuery(""); }}
                  className="accent-[#58a6ff] w-2.5 h-2.5"
                />
                <span className="text-[11px] text-[#c9d1d9] flex-1 truncate">
                  {countryFlag(data.code)} {data.name}
                </span>
                <span className="text-[9px] text-[#484f58] font-mono">
                  {data.code.toUpperCase()}
                </span>
                <span className="text-[10px] text-[#484f58]">
                  {data.airports.length}
                </span>
              </div>

              {isExpanded && (
                <div className="ml-5 space-y-px">
                  {data.airports.map((a) => (
                    <label
                      key={a.iata}
                      className={clsx(
                        "flex items-center gap-1.5 py-0.5 px-1 rounded text-[11px] cursor-pointer hover:bg-[#161b22]",
                        selectedCities.has(a.iata) ? "text-[#58a6ff]" : "text-[#8b949e]",
                      )}
                    >
                      <input
                        type="checkbox"
                        checked={selectedCities.has(a.iata)}
                        onChange={() => { toggleCity(a.iata); setQuery(""); }}
                        className="accent-[#58a6ff] w-2.5 h-2.5"
                      />
                      <span className="font-mono text-[10px] w-7">{a.iata}</span>
                      <span className="truncate">{a.city || a.name}</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>

    </div>
  );
}
