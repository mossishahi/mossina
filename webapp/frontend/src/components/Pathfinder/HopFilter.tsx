import { useState, useRef, useEffect } from "react";
import { MapPin } from "lucide-react";
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
  value: HopFilterValue;
  onChange: (val: HopFilterValue) => void;
}

export default function HopFilter({ index, value, onChange }: Props) {
  const [hovering, setHovering] = useState(false);
  const [pinned, setPinned] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout>>();
  const active = isHopActive(value);
  const show = hovering || pinned;

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setPinned(false);
        setHovering(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  function handleEnter() {
    clearTimeout(timerRef.current);
    setHovering(true);
  }
  function handleLeave() {
    timerRef.current = setTimeout(() => {
      if (!pinned) setHovering(false);
    }, 200);
  }

  return (
    <div
      className="relative"
      ref={ref}
      onMouseEnter={handleEnter}
      onMouseLeave={handleLeave}
    >
      <MapPin
        size={14}
        className={`cursor-pointer transition-colors ${
          active ? "text-[#58a6ff]" : "text-[#484f58] hover:text-[#8b949e]"
        }`}
        onClick={() => setPinned(!pinned)}
      />

      {show && (
        <div
          className="absolute left-1/2 -translate-x-1/2 top-full mt-1.5 z-50 bg-[#161b22] border border-[#30363d] rounded-lg shadow-xl w-52 p-2 space-y-2"
          onMouseEnter={handleEnter}
          onMouseLeave={handleLeave}
        >
          <div className="flex gap-2">
            <input
              type="number"
              min={0}
              max={30}
              placeholder="min days"
              value={value.minDays ?? ""}
              onChange={(e) =>
                onChange({ ...value, minDays: e.target.value === "" ? null : Number(e.target.value) })
              }
              className="flex-1 bg-[#0d1117] border border-[#30363d] rounded px-2 py-1 text-[10px] text-[#c9d1d9] outline-none focus:border-[#58a6ff] placeholder-[#484f58] [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none"
            />
            <input
              type="number"
              min={0}
              max={30}
              placeholder="max days"
              value={value.maxDays ?? ""}
              onChange={(e) =>
                onChange({ ...value, maxDays: e.target.value === "" ? null : Number(e.target.value) })
              }
              className="flex-1 bg-[#0d1117] border border-[#30363d] rounded px-2 py-1 text-[10px] text-[#c9d1d9] outline-none focus:border-[#58a6ff] placeholder-[#484f58] [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none"
            />
          </div>

          <CityTagInput
            placeholder="include cities"
            cities={value.includeCities}
            color="#3fb950"
            onAdd={(iata) =>
              onChange({ ...value, includeCities: [...value.includeCities, iata] })
            }
            onRemove={(iata) =>
              onChange({ ...value, includeCities: value.includeCities.filter((c) => c !== iata) })
            }
          />

          <CityTagInput
            placeholder="exclude cities"
            cities={value.excludeCities}
            color="#e5534b"
            onAdd={(iata) =>
              onChange({ ...value, excludeCities: [...value.excludeCities, iata] })
            }
            onRemove={(iata) =>
              onChange({ ...value, excludeCities: value.excludeCities.filter((c) => c !== iata) })
            }
          />
        </div>
      )}
    </div>
  );
}

function CityTagInput({
  placeholder,
  cities,
  color,
  onAdd,
  onRemove,
}: {
  placeholder: string;
  cities: string[];
  color: string;
  onAdd: (iata: string) => void;
  onRemove: (iata: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [focused, setFocused] = useState(false);
  const { data: airports = [] } = useAirports();

  const suggestions =
    focused && query.trim().length > 0
      ? airports
          .filter((a) => {
            const lq = query.trim().toLowerCase();
            return (
              !cities.includes(a.iata) &&
              (a.iata.toLowerCase().includes(lq) ||
                (a.city || "").toLowerCase().includes(lq) ||
                (a.name || "").toLowerCase().includes(lq))
            );
          })
          .slice(0, 5)
      : [];

  return (
    <div className="relative">
      <div className="flex items-center bg-[#0d1117] border border-[#30363d] rounded px-1.5 py-0.5 gap-1 overflow-x-auto no-scrollbar">
        {cities.map((c) => (
          <span
            key={c}
            className="inline-flex items-center gap-0.5 px-1 py-0.5 rounded text-[9px] font-medium whitespace-nowrap shrink-0"
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
          placeholder={cities.length === 0 ? placeholder : ""}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => setFocused(true)}
          onBlur={() => setTimeout(() => setFocused(false), 150)}
          className="flex-1 min-w-[40px] bg-transparent text-[10px] text-[#c9d1d9] outline-none placeholder-[#484f58] py-0.5"
        />
      </div>
      {suggestions.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-0.5 bg-[#1c2128] border border-[#30363d] rounded shadow-lg z-50 max-h-24 overflow-y-auto">
          {suggestions.map((a) => (
            <button
              key={a.iata}
              onMouseDown={(e) => {
                e.preventDefault();
                onAdd(a.iata);
                setQuery("");
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
