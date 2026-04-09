import { useMapStore } from "@/stores/mapStore";
import { AIRLINE_META } from "@/api/types";

const AIRLINES = Object.entries(AIRLINE_META);

export default function AirlineFilter() {
  const activeAirlines = useMapStore((s) => s.activeAirlines);
  const toggleAirline = useMapStore((s) => s.toggleAirline);

  return (
    <div className="flex items-center gap-3">
      <span className="text-xs font-medium text-[#8b949e] uppercase tracking-wider shrink-0">
        Airlines
      </span>
      {AIRLINES.map(([code, meta]) => {
        const active = activeAirlines.has(code);
        return (
          <label key={code} className="flex items-center gap-1.5 cursor-pointer select-none">
            <input
              type="checkbox"
              checked={active}
              onChange={() => toggleAirline(code)}
              className="w-3 h-3 rounded"
              style={{ accentColor: meta.color }}
            />
            <span
              className="text-[11px] font-medium"
              style={{ color: active ? meta.color : "#484f58" }}
            >
              {meta.name}
            </span>
          </label>
        );
      })}
    </div>
  );
}
