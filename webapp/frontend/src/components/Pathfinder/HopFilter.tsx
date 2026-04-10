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
            <div className="flex gap-1.5 items-center">
              <DayInput
                placeholder="min"
                value={value.minDays}
                onConfirm={(v) => onChange({ ...value, minDays: v })}
              />
              <DayInput
                placeholder="max"
                value={value.maxDays}
                onConfirm={(v) => onChange({ ...value, maxDays: v })}
              />
              <span className="text-[8px] text-[#484f58]">days</span>
            </div>
          )}
          <TagInput
            placeholder="include"
            tags={value.includeCities}
            color="#3fb950"
            onAdd={(c) => onChange({ ...value, includeCities: [...value.includeCities, c] })}
            onRemove={(c) => onChange({ ...value, includeCities: value.includeCities.filter((x) => x !== c) })}
            onAddMany={(cs) => onChange({ ...value, includeCities: [...new Set([...value.includeCities, ...cs])] })}
          />
          <TagInput
            placeholder="exclude"
            tags={value.excludeCities}
            color="#e5534b"
            onAdd={(c) => onChange({ ...value, excludeCities: [...value.excludeCities, c] })}
            onRemove={(c) => onChange({ ...value, excludeCities: value.excludeCities.filter((x) => x !== c) })}
            onAddMany={(cs) => onChange({ ...value, excludeCities: [...new Set([...value.excludeCities, ...cs])] })}
          />
        </div>
      )}
    </div>
  );
}

function DayInput({ placeholder, value, onConfirm }: {
  placeholder: string; value: number | null; onConfirm: (v: number | null) => void;
}) {
  const [draft, setDraft] = useState(value != null ? String(value) : "");
  const dirty = (draft === "" ? null : Number(draft)) !== value;

  useEffect(() => {
    setDraft(value != null ? String(value) : "");
  }, [value]);

  function confirm() {
    const v = draft.trim() === "" ? null : Math.max(0, Math.min(30, Number(draft)));
    onConfirm(v);
  }

  return (
    <div className={`flex items-center gap-0 rounded border overflow-hidden ${
      value != null ? "border-[#58a6ff]/30 bg-[#58a6ff]/10" : "border-[#21262d] bg-[#0d1117]"
    }`}>
      <input
        type="number"
        min={0}
        max={30}
        placeholder={placeholder}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onKeyDown={(e) => { if (e.key === "Enter") confirm(); }}
        className="w-8 bg-transparent text-[10px] text-[#c9d1d9] text-center outline-none placeholder-[#484f58] py-[2px] pl-1 [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none"
      />
      <button
        onClick={confirm}
        className={`px-1 py-[2px] transition-colors ${
          dirty ? "text-[#3fb950] hover:text-[#56d364]" : "text-[#30363d]"
        }`}
        title="Apply"
      >
        <svg width="8" height="8" viewBox="0 0 8 8">
          <path d="M1 4 L3 6.5 L7 1.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </button>
    </div>
  );
}


function TagInput({
  placeholder, tags, color, onAdd, onRemove, onAddMany,
}: {
  placeholder: string; tags: string[]; color: string;
  onAdd: (iata: string) => void; onRemove: (iata: string) => void;
  onAddMany?: (iatas: string[]) => void;
}) {
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);
  const { data: airports = [] } = useAirports();

  const { countryEntries, cityHits } = useMemo(() => {
    if (!focused || !q.trim()) return { countryEntries: [] as { name: string; code: string; cities: typeof airports }[], cityHits: [] as typeof airports };
    const lq = q.trim().toLowerCase();

    const countryMap = new Map<string, { name: string; code: string; cities: typeof airports }>();
    airports.forEach((a) => {
      if ((a.country || "").toLowerCase().includes(lq) ||
          (a.country_code || "").toLowerCase() === lq) {
        const key = a.country_code;
        if (!countryMap.has(key)) countryMap.set(key, { name: a.country || key, code: key, cities: [] });
        if (!tags.includes(a.iata)) countryMap.get(key)!.cities.push(a);
      }
    });

    if (countryMap.size > 0) {
      const entries = [...countryMap.values()].filter((e) => e.cities.length > 0);
      const topCities = entries.flatMap((e) => e.cities).slice(0, 6);
      return { countryEntries: entries, cityHits: topCities };
    }

    const cities = airports
      .filter((a) => !tags.includes(a.iata) &&
        (a.iata.toLowerCase().includes(lq) || (a.city || "").toLowerCase().includes(lq) || (a.name || "").toLowerCase().includes(lq)))
      .slice(0, 6);
    return { countryEntries: [], cityHits: cities };
  }, [focused, q, airports, tags]);

  const hasResults = countryEntries.length > 0 || cityHits.length > 0;

  function addAll(iatas: string[]) {
    if (onAddMany) onAddMany(iatas);
    else iatas.forEach((c) => onAdd(c));
    setQ("");
  }

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
      {hasResults && (
        <div className="absolute bottom-full left-0 right-0 mb-0.5 bg-[#1c2128] border border-[#30363d] rounded shadow-lg z-[110] max-h-40 overflow-y-auto">
          {countryEntries.map((entry) => (
            <button
              key={`country-${entry.code}`}
              onMouseDown={(e) => {
                e.preventDefault();
                addAll(entry.cities.map((a) => a.iata));
              }}
              className="w-full text-left px-2 py-1 text-[9px] hover:bg-[#21262d] font-semibold border-b border-[#30363d] flex items-center gap-1.5"
              style={{ color }}
            >
              <span className="text-[10px]">🌍</span>
              <span>{entry.name}</span>
              <span className="text-[#484f58] font-normal ml-auto">{entry.cities.length} cities</span>
            </button>
          ))}
          {cityHits.map((a) => (
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
