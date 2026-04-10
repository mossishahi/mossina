import { useState, useRef, useEffect } from "react";
import { MapPin, X } from "lucide-react";
import { useAirports } from "@/hooks/useAirports";

export interface HopFilterValue {
  minDays: number | null;
  maxDays: number | null;
  includeCities: string[];
  excludeCities: string[];
}

export const emptyHop = (): HopFilterValue => ({
  minDays: null,
  maxDays: null,
  includeCities: [],
  excludeCities: [],
});

export function isHopActive(v: HopFilterValue): boolean {
  return (
    v.minDays != null ||
    v.maxDays != null ||
    v.includeCities.length > 0 ||
    v.excludeCities.length > 0
  );
}

interface Props {
  index: number;
  label?: string;
  value: HopFilterValue;
  onChange: (val: HopFilterValue) => void;
}

export default function HopFilter({ index, label, value, onChange }: Props) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const active = isHopActive(value);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(!open)}
        className={`flex items-center gap-1 px-1.5 py-1 rounded-full border transition-all ${
          active
            ? "bg-[#58a6ff]/15 border-[#58a6ff]/40 text-[#58a6ff]"
            : "bg-[#161b22] border-[#30363d] text-[#8b949e] hover:border-[#484f58]"
        }`}
        title={label || `Hop ${index}`}
      >
        <MapPin size={10} className="shrink-0" />
        {label && <span className="text-[9px] font-medium">{label}</span>}
        {!label && value.includeCities.length > 0 && (
          <span className="text-[9px] font-medium">{value.includeCities.join(",")}</span>
        )}
        {!label && value.includeCities.length === 0 && (
          <span className="text-[9px]">{index}</span>
        )}
      </button>

      {open && (
        <HopPopover value={value} onChange={onChange} onClose={() => setOpen(false)} />
      )}
    </div>
  );
}

function HopPopover({
  value,
  onChange,
  onClose,
}: {
  value: HopFilterValue;
  onChange: (v: HopFilterValue) => void;
  onClose: () => void;
}) {
  const [includeQuery, setIncludeQuery] = useState("");
  const [excludeQuery, setExcludeQuery] = useState("");
  const [activeField, setActiveField] = useState<"include" | "exclude" | null>(null);
  const { data: airports = [] } = useAirports();

  function search(q: string) {
    if (q.trim().length === 0) return [];
    const lq = q.trim().toLowerCase();
    return airports
      .filter(
        (a) =>
          a.iata.toLowerCase().includes(lq) ||
          (a.city || "").toLowerCase().includes(lq) ||
          (a.name || "").toLowerCase().includes(lq),
      )
      .slice(0, 6);
  }

  function addCity(list: "include" | "exclude", iata: string) {
    if (list === "include") {
      if (!value.includeCities.includes(iata)) {
        onChange({ ...value, includeCities: [...value.includeCities, iata] });
      }
      setIncludeQuery("");
    } else {
      if (!value.excludeCities.includes(iata)) {
        onChange({ ...value, excludeCities: [...value.excludeCities, iata] });
      }
      setExcludeQuery("");
    }
  }

  function removeCity(list: "include" | "exclude", iata: string) {
    if (list === "include") {
      onChange({ ...value, includeCities: value.includeCities.filter((c) => c !== iata) });
    } else {
      onChange({ ...value, excludeCities: value.excludeCities.filter((c) => c !== iata) });
    }
  }

  return (
    <div className="absolute top-full left-1/2 -translate-x-1/2 mt-2 z-50 bg-[#161b22] border border-[#30363d] rounded-lg shadow-xl w-56 p-3 space-y-3">
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-semibold text-[#c9d1d9]">Stop filter</span>
        <button onClick={onClose} className="text-[#484f58] hover:text-[#c9d1d9]">
          <X size={12} />
        </button>
      </div>

      <div className="flex gap-2">
        <div className="flex-1">
          <label className="text-[9px] text-[#8b949e] uppercase tracking-wide">Min stay</label>
          <div className="flex items-center gap-1 mt-0.5">
            <input
              type="number"
              min={0}
              max={30}
              placeholder="-"
              value={value.minDays ?? ""}
              onChange={(e) =>
                onChange({ ...value, minDays: e.target.value === "" ? null : Number(e.target.value) })
              }
              className="w-full bg-[#0d1117] border border-[#30363d] rounded px-1.5 py-1 text-[10px] text-[#c9d1d9] text-center outline-none focus:border-[#58a6ff] [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none"
            />
            <span className="text-[9px] text-[#484f58]">days</span>
          </div>
        </div>
        <div className="flex-1">
          <label className="text-[9px] text-[#8b949e] uppercase tracking-wide">Max stay</label>
          <div className="flex items-center gap-1 mt-0.5">
            <input
              type="number"
              min={0}
              max={30}
              placeholder="-"
              value={value.maxDays ?? ""}
              onChange={(e) =>
                onChange({ ...value, maxDays: e.target.value === "" ? null : Number(e.target.value) })
              }
              className="w-full bg-[#0d1117] border border-[#30363d] rounded px-1.5 py-1 text-[10px] text-[#c9d1d9] text-center outline-none focus:border-[#58a6ff] [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none"
            />
            <span className="text-[9px] text-[#484f58]">days</span>
          </div>
        </div>
      </div>

      <CityTagField
        label="Include cities"
        cities={value.includeCities}
        query={includeQuery}
        setQuery={setIncludeQuery}
        active={activeField === "include"}
        onFocus={() => setActiveField("include")}
        suggestions={activeField === "include" ? search(includeQuery) : []}
        onAdd={(iata) => addCity("include", iata)}
        onRemove={(iata) => removeCity("include", iata)}
        color="#3fb950"
      />

      <CityTagField
        label="Exclude cities"
        cities={value.excludeCities}
        query={excludeQuery}
        setQuery={setExcludeQuery}
        active={activeField === "exclude"}
        onFocus={() => setActiveField("exclude")}
        suggestions={activeField === "exclude" ? search(excludeQuery) : []}
        onAdd={(iata) => addCity("exclude", iata)}
        onRemove={(iata) => removeCity("exclude", iata)}
        color="#e5534b"
      />
    </div>
  );
}

function CityTagField({
  label,
  cities,
  query,
  setQuery,
  active,
  onFocus,
  suggestions,
  onAdd,
  onRemove,
  color,
}: {
  label: string;
  cities: string[];
  query: string;
  setQuery: (v: string) => void;
  active: boolean;
  onFocus: () => void;
  suggestions: { iata: string; city: string; name: string }[];
  onAdd: (iata: string) => void;
  onRemove: (iata: string) => void;
  color: string;
}) {
  return (
    <div className="relative">
      <label className="text-[9px] text-[#8b949e] uppercase tracking-wide">{label}</label>
      <div className="flex flex-wrap gap-1 mt-0.5 bg-[#0d1117] border border-[#30363d] rounded p-1 min-h-[26px] items-center">
        {cities.map((c) => (
          <span
            key={c}
            className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-medium"
            style={{ background: `${color}20`, color }}
          >
            {c}
            <button
              onClick={() => onRemove(c)}
              className="hover:opacity-70 text-[8px] leading-none ml-0.5"
            >
              ×
            </button>
          </span>
        ))}
        <input
          type="text"
          placeholder={cities.length === 0 ? "Search..." : ""}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={onFocus}
          className="flex-1 min-w-[40px] bg-transparent text-[10px] text-[#c9d1d9] outline-none placeholder-[#30363d]"
        />
      </div>
      {active && suggestions.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-0.5 bg-[#1c2128] border border-[#30363d] rounded shadow-lg z-50 max-h-28 overflow-y-auto">
          {suggestions
            .filter((a) => !cities.includes(a.iata))
            .map((a) => (
              <button
                key={a.iata}
                onMouseDown={(e) => {
                  e.preventDefault();
                  onAdd(a.iata);
                }}
                className="w-full text-left px-2 py-1 text-[9px] hover:bg-[#21262d] flex items-center gap-1"
              >
                <span className="font-mono font-semibold" style={{ color }}>
                  {a.iata}
                </span>
                <span className="text-[#8b949e] truncate">{a.city || a.name}</span>
              </button>
            ))}
        </div>
      )}
    </div>
  );
}
