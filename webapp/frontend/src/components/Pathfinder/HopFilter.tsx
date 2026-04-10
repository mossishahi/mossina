import { useState, useRef, useEffect, useCallback, useMemo } from "react";
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
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout>>();
  const active = isHopActive(value);

  const keep = useCallback(() => clearTimeout(closeTimer.current), []);
  const scheduleClose = useCallback(() => {
    clearTimeout(closeTimer.current);
    closeTimer.current = setTimeout(() => setOpen(false), 250);
  }, []);

  useEffect(() => () => clearTimeout(closeTimer.current), []);

  useEffect(() => {
    function click(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node))
        setOpen(false);
    }
    document.addEventListener("mousedown", click);
    return () => document.removeEventListener("mousedown", click);
  }, []);

  return (
    <div
      ref={wrapRef}
      className="relative inline-flex items-center"
      onMouseEnter={() => { keep(); setOpen(true); }}
      onMouseLeave={scheduleClose}
    >
      <MapPin
        size={13}
        strokeWidth={2.2}
        className={`cursor-pointer transition-colors ${
          active ? "text-[#58a6ff]" : "text-[#484f58] hover:text-[#8b949e]"
        }`}
      />

      {open && (
        <div
          className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 z-[100] bg-[#161b22]/95 backdrop-blur border border-[#30363d] rounded-lg shadow-2xl w-48 p-2 space-y-1.5"
          onMouseEnter={keep}
          onMouseLeave={scheduleClose}
        >
          <div className="flex gap-1.5">
            <input
              type="number" min={0} max={30}
              placeholder="min days"
              value={value.minDays ?? ""}
              onChange={(e) => onChange({ ...value, minDays: e.target.value === "" ? null : Number(e.target.value) })}
              className="flex-1 w-0 bg-[#0d1117] border border-[#21262d] rounded px-1.5 py-[3px] text-[10px] text-[#c9d1d9] outline-none focus:border-[#58a6ff] placeholder-[#484f58] [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none"
            />
            <input
              type="number" min={0} max={30}
              placeholder="max days"
              value={value.maxDays ?? ""}
              onChange={(e) => onChange({ ...value, maxDays: e.target.value === "" ? null : Number(e.target.value) })}
              className="flex-1 w-0 bg-[#0d1117] border border-[#21262d] rounded px-1.5 py-[3px] text-[10px] text-[#c9d1d9] outline-none focus:border-[#58a6ff] placeholder-[#484f58] [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none"
            />
          </div>
          <TagInput
            placeholder="include"
            tags={value.includeCities}
            color="#3fb950"
            onAdd={(c) => onChange({ ...value, includeCities: [...value.includeCities, c] })}
            onRemove={(c) => onChange({ ...value, includeCities: value.includeCities.filter((x) => x !== c) })}
          />
          <TagInput
            placeholder="exclude"
            tags={value.excludeCities}
            color="#e5534b"
            onAdd={(c) => onChange({ ...value, excludeCities: [...value.excludeCities, c] })}
            onRemove={(c) => onChange({ ...value, excludeCities: value.excludeCities.filter((x) => x !== c) })}
          />
        </div>
      )}
    </div>
  );
}

function TagInput({
  placeholder, tags, color, onAdd, onRemove,
}: {
  placeholder: string; tags: string[]; color: string;
  onAdd: (iata: string) => void; onRemove: (iata: string) => void;
}) {
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);
  const { data: airports = [] } = useAirports();

  const hits = useMemo(() => {
    if (!focused || !q.trim()) return [] as typeof airports;
    const lq = q.trim().toLowerCase();
    const countryMatch = new Set<string>();
    airports.forEach((a) => {
      if ((a.country || "").toLowerCase().includes(lq) ||
          (a.country_code || "").toLowerCase() === lq)
        countryMatch.add(a.country_code);
    });
    if (countryMatch.size > 0) {
      return airports.filter(
        (a) => countryMatch.has(a.country_code) && !tags.includes(a.iata),
      );
    }
    return airports
      .filter((a) => !tags.includes(a.iata) &&
        (a.iata.toLowerCase().includes(lq) || (a.city || "").toLowerCase().includes(lq) || (a.name || "").toLowerCase().includes(lq)))
      .slice(0, 6);
  }, [focused, q, airports, tags]);

  return (
    <div className="relative">
      <div className="flex items-center gap-0.5 bg-[#0d1117] border border-[#21262d] rounded px-1 py-[2px] overflow-x-auto no-scrollbar">
        {tags.map((t) => (
          <span key={t} className="shrink-0 inline-flex items-center gap-px px-1 rounded text-[8px] font-semibold" style={{ background: `${color}18`, color }}>
            {t}
            <button onClick={() => onRemove(t)} className="ml-0.5 text-[7px] opacity-70 hover:opacity-100">×</button>
          </span>
        ))}
        <input
          type="text"
          placeholder={tags.length === 0 ? placeholder : ""}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onFocus={() => setFocused(true)}
          onBlur={() => setTimeout(() => setFocused(false), 150)}
          className="flex-1 min-w-[30px] bg-transparent text-[9px] text-[#c9d1d9] outline-none placeholder-[#484f58] py-px"
        />
      </div>
      {hits.length > 0 && (
        <div className="absolute bottom-full left-0 right-0 mb-0.5 bg-[#1c2128] border border-[#30363d] rounded shadow-lg z-[110] max-h-32 overflow-y-auto">
          {hits.length > 3 && (
            <button
              onMouseDown={(e) => {
                e.preventDefault();
                hits.forEach((a) => onAdd(a.iata));
                setQ("");
              }}
              className="w-full text-left px-2 py-0.5 text-[9px] hover:bg-[#21262d] font-semibold border-b border-[#30363d]"
              style={{ color }}
            >
              + Add all {hits.length} cities
            </button>
          )}
          {hits.slice(0, 8).map((a) => (
            <button
              key={a.iata}
              onMouseDown={(e) => { e.preventDefault(); onAdd(a.iata); setQ(""); }}
              className="w-full text-left px-2 py-0.5 text-[9px] hover:bg-[#21262d] flex items-center gap-1"
            >
              <span className="font-mono font-bold" style={{ color }}>{a.iata}</span>
              <span className="text-[#8b949e] truncate">{a.city || a.name}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
