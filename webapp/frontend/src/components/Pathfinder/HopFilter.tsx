import { useState, useRef, useEffect } from "react";
import { Clock, MapPin, Lock } from "lucide-react";
import { useAirports } from "@/hooks/useAirports";

export interface HopFilterValue {
  minHours: number | null;
  city: string | null;
}

interface Props {
  index: number;
  total: number;
  locked: boolean;
  lockedLabel?: string;
  value: HopFilterValue;
  onChange: (val: HopFilterValue) => void;
}

export default function HopFilter({ index, total, locked, lockedLabel, value, onChange }: Props) {
  const [cityQuery, setCityQuery] = useState("");
  const [showSuggestions, setShowSuggestions] = useState(false);
  const sugRef = useRef<HTMLDivElement>(null);
  const { data: airports = [] } = useAirports();

  const suggestions = cityQuery.trim().length > 0
    ? airports.filter((a) => {
        const q = cityQuery.trim().toLowerCase();
        return a.iata.toLowerCase().includes(q) ||
          (a.city || "").toLowerCase().includes(q) ||
          (a.name || "").toLowerCase().includes(q);
      }).slice(0, 6)
    : [];

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (sugRef.current && !sugRef.current.contains(e.target as Node)) {
        setShowSuggestions(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  if (locked) {
    return (
      <div className="flex items-center gap-1 px-2 py-1 bg-[#161b22] rounded-full border border-[#21262d]">
        <Lock size={8} className="text-[#30363d] shrink-0" />
        <span className="text-[9px] text-[#484f58] truncate">
          {lockedLabel || `Stop ${index + 1}`}
        </span>
      </div>
    );
  }

  return (
    <div className="flex items-center bg-[#0d1117] border border-[#30363d] rounded-full overflow-visible min-w-0">
      <div
        className="flex items-center gap-0.5 px-1.5 py-0.5 border-r border-[#30363d]"
        title="Min stay (hours) at this stop"
      >
        <Clock size={8} className="text-[#484f58] shrink-0" />
        <input
          type="number"
          min={0}
          max={720}
          placeholder="h"
          value={value.minHours ?? ""}
          onChange={(e) => {
            const v = e.target.value === "" ? null : Number(e.target.value);
            onChange({ ...value, minHours: v });
          }}
          className="w-7 bg-transparent text-[9px] text-[#c9d1d9] text-center outline-none placeholder-[#30363d] [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
        />
      </div>
      <div className="relative flex items-center gap-0.5 px-1.5 py-0.5 min-w-0" ref={sugRef}>
        <MapPin size={8} className="text-[#484f58] shrink-0" />
        {value.city ? (
          <span className="flex items-center gap-0.5">
            <span className="text-[9px] text-[#58a6ff] font-medium">{value.city}</span>
            <button
              onClick={() => { onChange({ ...value, city: null }); setCityQuery(""); }}
              className="text-[#484f58] hover:text-[#e5534b] text-[8px] leading-none"
            >
              x
            </button>
          </span>
        ) : (
          <input
            type="text"
            placeholder="city"
            value={cityQuery}
            onChange={(e) => { setCityQuery(e.target.value); setShowSuggestions(true); }}
            onFocus={() => setShowSuggestions(true)}
            className="w-12 bg-transparent text-[9px] text-[#c9d1d9] outline-none placeholder-[#30363d] min-w-0"
          />
        )}
        {showSuggestions && suggestions.length > 0 && !value.city && (
          <div className="absolute top-full left-0 mt-1 bg-[#161b22] border border-[#30363d] rounded shadow-lg z-50 max-h-32 overflow-y-auto w-40">
            {suggestions.map((a) => (
              <button
                key={a.iata}
                onClick={() => {
                  onChange({ ...value, city: a.iata });
                  setCityQuery("");
                  setShowSuggestions(false);
                }}
                className="w-full text-left px-2 py-1 text-[9px] hover:bg-[#21262d] flex items-center gap-1"
              >
                <span className="text-[#58a6ff] font-mono font-semibold">{a.iata}</span>
                <span className="text-[#8b949e] truncate">{a.city || a.name}</span>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
