import AirlineFilter from "./AirlineFilter";
import CountryTree from "./CountryTree";
import TimeFrame from "./TimeFrame";
import { useMapStore } from "@/stores/mapStore";
import { useRoutes } from "@/hooks/useRoutes";

export default function Sidebar() {
  const selectedCities = useMapStore((s) => s.selectedCities);
  const { data: routes = [] } = useRoutes();

  const relevantRoutes = routes.filter(
    (r) =>
      selectedCities.has(r.origin) || selectedCities.has(r.destination),
  );

  return (
    <div className="bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden">
      <div className="px-4 pt-3 pb-2 border-b border-[#30363d] space-y-2">
        <div className="flex items-center justify-between">
          <h1 className="text-base font-semibold text-white tracking-tight">
            Mossina
          </h1>
          <p className="text-[10px] text-[#484f58]">
            {selectedCities.size} cities
            {selectedCities.size > 0 && ` / ${relevantRoutes.length} routes`}
          </p>
        </div>
        <AirlineFilter />
        <TimeFrame />
      </div>

      <div className="p-3 max-h-[50vh] overflow-y-auto">
        <CountryTree />
      </div>
    </div>
  );
}
