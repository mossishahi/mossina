import { useQuery } from "@tanstack/react-query";
import { useMapStore } from "@/stores/mapStore";
import { getAirlines } from "@/api/client";
import { AIRLINE_META } from "@/api/types";

const AIRLINES = Object.entries(AIRLINE_META);

function formatAgo(iso: string): string {
  const ms = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(ms / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ${mins % 60}m ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ${hrs % 24}h ago`;
}

export default function AirlineFilter() {
  const activeAirlines = useMapStore((s) => s.activeAirlines);
  const toggleAirline = useMapStore((s) => s.toggleAirline);
  const { data: airlineData } = useQuery({
    queryKey: ["airlines"],
    queryFn: getAirlines,
    staleTime: 60_000,
  });

  const lastScraped: Record<string, string | null> = {};
  airlineData?.forEach((a) => { lastScraped[a.code] = a.last_scraped; });

  return (
    <div className="flex items-center gap-3">
      <span className="text-xs font-medium text-[#8b949e] uppercase tracking-wider shrink-0">
        Airlines
      </span>
      {AIRLINES.map(([code, meta]) => {
        const active = activeAirlines.has(code);
        const ls = lastScraped[code];
        const tooltip = ls
          ? `${meta.name} — last updated ${formatAgo(ls)}\n${new Date(ls).toLocaleString()}`
          : meta.name;
        return (
          <label key={code} className="flex items-center gap-1.5 cursor-pointer select-none" title={tooltip}>
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
