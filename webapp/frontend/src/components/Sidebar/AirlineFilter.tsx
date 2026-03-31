import { useMapStore } from "@/stores/mapStore";
import { AIRLINE_META } from "@/api/types";

const AIRLINES = Object.entries(AIRLINE_META);

export default function AirlineFilter() {
  const activeAirlines = useMapStore((s) => s.activeAirlines);
  const toggleAirline = useMapStore((s) => s.toggleAirline);

  return (
    <div>
      <h3 className="text-xs font-medium text-[#8b949e] uppercase tracking-wider mb-2">
        Airlines
      </h3>
      <div className="flex gap-2">
        {AIRLINES.map(([code, meta]) => {
          const active = activeAirlines.has(code);
          return (
            <button
              key={code}
              onClick={() => toggleAirline(code)}
              className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm border transition-all"
              style={{
                borderColor: active ? meta.color : "#30363d",
                backgroundColor: active ? `${meta.color}20` : "transparent",
                color: active ? meta.color : "#8b949e",
              }}
            >
              <span
                className="w-2 h-2 rounded-full"
                style={{ backgroundColor: meta.color, opacity: active ? 1 : 0.4 }}
              />
              {meta.name}
            </button>
          );
        })}
      </div>
    </div>
  );
}
