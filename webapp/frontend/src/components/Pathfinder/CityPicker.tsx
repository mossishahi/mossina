import { useState, useRef, useEffect, useMemo } from "react";
import { X } from "lucide-react";
import type { Airport } from "@/api/types";

interface Props {
  selected: Set<string>;
  onToggle: (iata: string) => void;
  airports: Airport[];
  placeholder: string;
  maxSelect?: number;
}

export default function CityPicker({ selected, onToggle, airports, placeholder, maxSelect }: Props) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
        setQuery("");
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const nameMap = useMemo(() => {
    const m = new Map<string, string>();
    airports.forEach((a) => m.set(a.iata, a.city || a.name || a.iata));
    return m;
  }, [airports]);

  const grouped = useMemo(() => {
    const map = new Map<string, { countryName: string; airports: Airport[] }>();
    airports.forEach((a) => {
      if (!map.has(a.country_code)) {
        map.set(a.country_code, { countryName: a.country || a.country_code, airports: [] });
      }
      map.get(a.country_code)!.airports.push(a);
    });
    return [...map.entries()]
      .sort((a, b) => a[1].countryName.localeCompare(b[1].countryName))
      .map(([code, { countryName, airports: aps }]) => ({
        code,
        countryName,
        airports: [...aps].sort((a, b) =>
          (a.city || a.name).localeCompare(b.city || b.name),
        ),
      }));
  }, [airports]);

  const filtered = useMemo(() => {
    if (!query.trim()) return grouped;
    const q = query.toLowerCase();
    return grouped
      .map((g) => ({
        ...g,
        airports: g.airports.filter(
          (a) =>
            a.city?.toLowerCase().includes(q) ||
            a.iata.toLowerCase().includes(q) ||
            g.countryName.toLowerCase().includes(q),
        ),
      }))
      .filter((g) => g.airports.length > 0);
  }, [grouped, query]);

  const atMax = maxSelect != null && selected.size >= maxSelect;

  return (
    <div ref={containerRef} className="relative">
      {/* Trigger: chips + search input */}
      <div
        className="min-h-[32px] flex flex-wrap gap-1 px-2 py-1 bg-[#0d1117] border border-[#30363d] rounded-md cursor-text hover:border-[#484f58] transition-colors"
        onClick={() => setOpen(true)}
      >
        {[...selected].map((iata) => (
          <span
            key={iata}
            className="inline-flex items-center gap-0.5 bg-[#1c2a3a] text-[#58a6ff] text-[10px] px-1.5 py-0.5 rounded"
          >
            {nameMap.get(iata) || iata}
            <button
              onClick={(e) => { e.stopPropagation(); onToggle(iata); }}
              className="text-[#58a6ff]/60 hover:text-white ml-0.5"
            >
              <X size={9} />
            </button>
          </span>
        ))}
        <input
          className="flex-1 min-w-[60px] bg-transparent text-xs text-[#c9d1d9] placeholder-[#484f58] outline-none py-0.5"
          placeholder={selected.size === 0 ? placeholder : ""}
          value={query}
          onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
          onFocus={() => setOpen(true)}
        />
      </div>

      {/* Dropdown */}
      {open && (
        <div className="absolute z-50 top-full mt-1 left-0 right-0 bg-[#161b22] border border-[#30363d] rounded-md shadow-xl max-h-60 overflow-y-auto">
          {filtered.length === 0 ? (
            <p className="text-[11px] text-[#484f58] text-center py-3">No cities found</p>
          ) : (
            filtered.map((group) => (
              <div key={group.code}>
                <div className="px-2 py-1 text-[9px] font-semibold text-[#484f58] uppercase tracking-wider sticky top-0 bg-[#161b22] border-b border-[#21262d]">
                  {group.countryName}
                </div>
                {group.airports.map((a) => {
                  const isSel = selected.has(a.iata);
                  const disabled = !isSel && atMax;
                  return (
                    <button
                      key={a.iata}
                      onClick={() => { if (!disabled) { onToggle(a.iata); if (!isSel) setQuery(""); } }}
                      disabled={disabled}
                      className={`w-full flex items-center gap-2 px-3 py-1.5 text-left transition-colors ${
                        isSel
                          ? "bg-[#58a6ff]/10 text-[#58a6ff]"
                          : disabled
                          ? "text-[#2d333b] cursor-not-allowed"
                          : "text-[#c9d1d9] hover:bg-[#21262d]"
                      }`}
                    >
                      <span className="text-[10px] font-mono text-[#8b949e] w-7 shrink-0">
                        {a.iata}
                      </span>
                      <span className="text-xs truncate">{a.city || a.name}</span>
                      {isSel && (
                        <span className="ml-auto text-[10px] text-[#58a6ff]">✓</span>
                      )}
                    </button>
                  );
                })}
              </div>
            ))
          )}
          {atMax && (
            <p className="text-[10px] text-[#484f58] text-center py-1.5 border-t border-[#21262d]">
              Max {maxSelect} selected
            </p>
          )}
        </div>
      )}
    </div>
  );
}
