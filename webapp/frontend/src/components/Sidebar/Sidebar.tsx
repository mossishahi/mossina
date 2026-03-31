import { Compass } from "lucide-react";
import SearchBar from "./SearchBar";
import AirlineFilter from "./AirlineFilter";
import CountryTree from "./CountryTree";
import TimeFrame from "./TimeFrame";
import { useMapStore } from "@/stores/mapStore";
import { useRoutes } from "@/hooks/useRoutes";

interface Props {
  onOpenPathfinder: () => void;
}

export default function Sidebar({ onOpenPathfinder }: Props) {
  const selectedCities = useMapStore((s) => s.selectedCities);
  const { data: routes = [] } = useRoutes();

  const relevantRoutes = routes.filter(
    (r) =>
      selectedCities.has(r.origin) || selectedCities.has(r.destination),
  );

  return (
    <div className="absolute top-4 left-4 bottom-4 w-80 bg-black/80 backdrop-blur-xl border border-[#30363d] rounded-xl flex flex-col overflow-hidden z-10">
      <div className="p-4 border-b border-[#30363d]">
        <h1 className="text-lg font-semibold text-white tracking-tight">
          Mossina
        </h1>
        <p className="text-xs text-[#8b949e] mt-0.5">
          {selectedCities.size} cities selected
          {selectedCities.size > 0 && ` / ${relevantRoutes.length} routes`}
        </p>
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        <SearchBar />
        <AirlineFilter />
        <TimeFrame />
        <CountryTree />
      </div>

      <div className="p-3 border-t border-[#30363d]">
        <button
          onClick={onOpenPathfinder}
          className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-[#58a6ff]/10 hover:bg-[#58a6ff]/20 text-[#58a6ff] border border-[#58a6ff]/30 rounded-lg text-sm font-medium transition-colors"
        >
          <Compass size={16} />
          Pathfinder
        </button>
      </div>
    </div>
  );
}
