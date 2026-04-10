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
  isEndpoint?: boolean;
  value: HopFilterValue;
  onChange: (val: HopFilterValue) => void;
}

export default function HopFilter({ index, isEndpoint, value, onChange }: Props) {
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
          {!isEndpoint && (
            <div className="flex gap-1 items-center">
              <SpinSelector
                label="min"
                value={value.minDays}
                onChange={(v) => onChange({ ...value, minDays: v })}
              />
              <span className="text-[8px] text-[#484f58]">–</span>
              <SpinSelector
                label="max"
                value={value.maxDays}
                onChange={(v) => onChange({ ...value, maxDays: v })}
              />
              <span className="text-[8px] text-[#484f58] ml-0.5">days</span>
            </div>
          )}
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

const SPIN_VALUES = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 21, 30];

function SpinSelector({ label, value, onChange }: {
  label: string; value: number | null; onChange: (v: number | null) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const allValues = [null, ...SPIN_VALUES];
  const currentIdx = value == null ? 0 : allValues.indexOf(value);

  function scrollTo(idx: number) {
    const clamped = Math.max(0, Math.min(idx, allValues.length - 1));
    onChange(allValues[clamped]);
  }

  function handleWheel(e: React.WheelEvent) {
    e.preventDefault();
    e.stopPropagation();
    if (e.deltaY > 0) scrollTo(currentIdx + 1);
    else if (e.deltaY < 0) scrollTo(currentIdx - 1);
  }

  return (
    <div
      ref={containerRef}
      onWheel={handleWheel}
      className="relative flex flex-col items-center w-10 select-none"
    >
      <button
        onClick={() => scrollTo(currentIdx - 1)}
        className="text-[#484f58] hover:text-[#8b949e] h-3 flex items-center justify-center"
        disabled={currentIdx <= 0}
      >
        <svg width="8" height="5" viewBox="0 0 8 5">
          <path d="M1 4 L4 1 L7 4" fill="none" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
        </svg>
      </button>

      <div className={`w-full text-center py-px rounded text-[10px] font-semibold tabular-nums ${
        value != null
          ? "bg-[#58a6ff]/15 text-[#58a6ff]"
          : "text-[#484f58]"
      }`}>
        {value != null ? value : label}
      </div>

      <button
        onClick={() => scrollTo(currentIdx + 1)}
        className="text-[#484f58] hover:text-[#8b949e] h-3 flex items-center justify-center"
        disabled={currentIdx >= allValues.length - 1}
      >
        <svg width="8" height="5" viewBox="0 0 8 5">
          <path d="M1 1 L4 4 L7 1" fill="none" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
        </svg>
      </button>
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
