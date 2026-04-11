import { useState, useRef, useEffect, useMemo } from "react";
import { createPortal } from "react-dom";
import { X } from "lucide-react";
import type { Airport } from "@/api/types";

interface Props {
  selected: Set<string>;
  onToggle: (iata: string) => void;
  airports: Airport[];
  placeholder: string;
  maxSelect?: number;
  /** When provided, airports NOT in this set are dimmed and shown in a separate section */
  reachable?: Set<string>;
}

interface DropdownPos {
  top: number;
  left: number;
  width: number;
}

export default function CityPicker({ selected, onToggle, airports, placeholder, maxSelect, reachable }: Props) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<DropdownPos>({ top: 0, left: 0, width: 0 });
  const triggerRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      const target = e.target as Node;
      if (
        triggerRef.current?.contains(target) ||
        dropdownRef.current?.contains(target)
      ) return;
      setOpen(false);
      setQuery("");
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  // Close on scroll only if scroll happens OUTSIDE the dropdown
  useEffect(() => {
    if (!open) return;
    function handleScroll(e: Event) {
      if (dropdownRef.current?.contains(e.target as Node)) return;
      setOpen(false);
      setQuery("");
    }
    window.addEventListener("scroll", handleScroll, true);
    return () => window.removeEventListener("scroll", handleScroll, true);
  }, [open]);

  function openDropdown() {
    if (triggerRef.current) {
      const rect = triggerRef.current.getBoundingClientRect();
      setPos({ top: rect.bottom + 4, left: rect.left, width: rect.width });
    }
    setOpen(true);
  }

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

  // When reachable set is provided, split into two sections
  const { reachableGroups, otherGroups } = useMemo(() => {
    if (!reachable) return { reachableGroups: filtered, otherGroups: [] };
    const rg = filtered
      .map((g) => ({ ...g, airports: g.airports.filter((a) => reachable.has(a.iata)) }))
      .filter((g) => g.airports.length > 0);
    const og = filtered
      .map((g) => ({ ...g, airports: g.airports.filter((a) => !reachable.has(a.iata)) }))
      .filter((g) => g.airports.length > 0);
    return { reachableGroups: rg, otherGroups: og };
  }, [filtered, reachable]);

  const atMax = maxSelect != null && selected.size >= maxSelect;

  function renderAirport(a: Airport, keyPrefix = "") {
    const isSel = selected.has(a.iata);
    const disabled = !isSel && atMax;
    return (
      <button
        key={keyPrefix + a.iata}
        onMouseDown={(e) => {
          e.preventDefault();
          if (!disabled) { onToggle(a.iata); if (!isSel) setQuery(""); }
        }}
        disabled={disabled}
        className={`w-full flex items-center gap-2 px-3 py-2 text-left transition-colors ${
          isSel
            ? "bg-[#58a6ff]/10 text-[#58a6ff]"
            : disabled
            ? "text-[#484f58] cursor-not-allowed"
            : "text-[#c9d1d9] hover:bg-[#21262d]"
        }`}
      >
        <span className={`text-xs font-mono w-8 shrink-0 ${isSel ? "text-[#58a6ff]" : disabled ? "text-[#3d444d]" : "text-[#8b949e]"}`}>
          {a.iata}
        </span>
        <span className="text-sm truncate">{a.city || a.name}</span>
        {isSel && <span className="ml-auto text-xs text-[#58a6ff]">✓</span>}
      </button>
    );
  }

  function renderGroup(group: typeof filtered[0], keyPrefix = "") {
    return (
      <div key={keyPrefix + group.code}>
        <div className="px-2 py-1.5 text-[10px] font-semibold uppercase tracking-wider sticky top-0 border-b border-[#21262d] text-[#484f58] bg-[#161b22]">
          {group.countryName}
        </div>
        {group.airports.map((a) => renderAirport(a, keyPrefix))}
      </div>
    );
  }

  const dropdown = open && createPortal(
    <div
      ref={dropdownRef}
      style={{
        position: "fixed",
        top: pos.top,
        left: pos.left,
        width: pos.width,
        zIndex: 9999,
        maxHeight: "340px",
      }}
      className="bg-[#161b22] border border-[#30363d] rounded-md shadow-xl overflow-y-auto"
    >
      {reachableGroups.length === 0 && otherGroups.length === 0 && (
        <p className="text-xs text-[#484f58] text-center py-4">No cities found</p>
      )}

      {reachable && reachableGroups.length > 0 && (
        <div className="px-2 pt-2 pb-1">
          <span className="text-[10px] font-semibold text-[#3fb950] uppercase tracking-wider">
            Direct flights available
          </span>
        </div>
      )}
      {reachableGroups.map((g) => renderGroup(g))}

      {reachable && otherGroups.length > 0 && (
        <div className={`px-2 pt-2 pb-1 ${reachableGroups.length > 0 ? "border-t border-[#21262d] mt-1" : ""}`}>
          <span className="text-[10px] font-semibold text-[#484f58] uppercase tracking-wider">
            Other cities
          </span>
        </div>
      )}
      {otherGroups.map((g) => renderGroup(g, "other-"))}

      {atMax && (
        <p className="text-xs text-[#484f58] text-center py-2 border-t border-[#21262d]">
          Max {maxSelect} selected
        </p>
      )}
    </div>,
    document.body,
  );

  return (
    <div ref={triggerRef} className="relative">
      <div
        className="min-h-[38px] flex flex-wrap gap-1 px-2 py-1.5 bg-[#0d1117] border border-[#30363d] rounded-md cursor-text hover:border-[#484f58] transition-colors"
        onClick={openDropdown}
      >
        {[...selected].map((iata) => (
          <span
            key={iata}
            className="inline-flex items-center gap-1 bg-[#1c2a3a] text-[#58a6ff] text-xs px-2 py-0.5 rounded"
          >
            {nameMap.get(iata) || iata}
            <button
              onMouseDown={(e) => { e.stopPropagation(); onToggle(iata); }}
              className="text-[#58a6ff]/60 hover:text-white ml-0.5"
            >
              <X size={10} />
            </button>
          </span>
        ))}
        <input
          className="flex-1 min-w-[80px] bg-transparent text-sm text-[#c9d1d9] placeholder-[#484f58] outline-none py-0.5"
          placeholder={selected.size === 0 ? placeholder : ""}
          value={query}
          onChange={(e) => { setQuery(e.target.value); openDropdown(); }}
          onFocus={openDropdown}
        />
      </div>
      {dropdown}
    </div>
  );
}
