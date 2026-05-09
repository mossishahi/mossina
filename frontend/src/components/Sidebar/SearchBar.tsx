import { useState, useRef, useEffect } from "react";
import { Search, X } from "lucide-react";
import { searchAirports } from "@/api/client";
import { useMapStore } from "@/stores/mapStore";
import type { Airport } from "@/api/types";

export default function SearchBar() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<Airport[]>([]);
  const [open, setOpen] = useState(false);
  const toggleCity = useMapStore((s) => s.toggleCity);
  const timerRef = useRef<ReturnType<typeof setTimeout>>();
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!query.trim()) {
      setResults([]);
      setOpen(false);
      return;
    }
    clearTimeout(timerRef.current);
    timerRef.current = setTimeout(async () => {
      try {
        const data = await searchAirports(query);
        setResults(data);
        setOpen(true);
      } catch {
        setResults([]);
      }
    }, 250);
    return () => clearTimeout(timerRef.current);
  }, [query]);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function handleSelect(airport: Airport) {
    toggleCity(airport.iata);
    setQuery("");
    setOpen(false);
  }

  return (
    <div ref={containerRef} className="relative">
      <div className="relative">
        <Search
          size={14}
          className="absolute left-3 top-1/2 -translate-y-1/2 text-[#8b949e]"
        />
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search airports or cities..."
          className="w-full bg-[#161b22] border border-[#30363d] rounded-lg py-2 pl-9 pr-8 text-sm text-[#c9d1d9] placeholder-[#484f58] focus:outline-none focus:border-[#58a6ff] transition-colors"
        />
        {query && (
          <button
            onClick={() => {
              setQuery("");
              setOpen(false);
            }}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-[#8b949e] hover:text-[#c9d1d9]"
          >
            <X size={14} />
          </button>
        )}
      </div>

      {open && results.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-[#161b22] border border-[#30363d] rounded-lg shadow-xl max-h-60 overflow-y-auto z-50">
          {results.map((a) => (
            <button
              key={a.iata}
              onClick={() => handleSelect(a)}
              className="w-full text-left px-3 py-2 text-sm hover:bg-[#30363d]/50 transition-colors flex items-center gap-2"
            >
              <span className="text-[#58a6ff] font-mono text-xs font-semibold w-8">
                {a.iata}
              </span>
              <span className="text-[#c9d1d9] truncate flex-1">
                {a.name}
              </span>
              <span className="text-[#8b949e] text-xs">{a.country}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
