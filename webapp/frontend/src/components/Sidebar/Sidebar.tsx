import AirlineFilter from "./AirlineFilter";
import TimeFrame from "./TimeFrame";
import { useMapStore } from "@/stores/mapStore";
import { useRoutes } from "@/hooks/useRoutes";

export default function Sidebar() {
  const originCities = useMapStore((s) => s.originCities);
  const destinationCities = useMapStore((s) => s.destinationCities);
  const { data: routes = [] } = useRoutes();

  const allSelected = new Set([...originCities, ...destinationCities]);
  const relevantRoutes = routes.filter(
    (r) => allSelected.has(r.origin) || allSelected.has(r.destination),
  );

  return (
    <div className="bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden">
      <div className="px-4 pt-3.5 pb-3.5 space-y-3">
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold text-white tracking-tight">
            Mossina
          </h1>
          <p className="text-xs text-[#484f58]">
            {originCities.size > 0 && (
              <span>
                <span className="text-[#58a6ff]">{originCities.size}</span>
                {" → "}
                <span className="text-[#3fb950]">{destinationCities.size}</span>
              </span>
            )}
            {allSelected.size > 0 && ` · ${relevantRoutes.length} routes`}
          </p>
        </div>
        <AirlineFilter />
        <TimeFrame />
      </div>
    </div>
  );
}
