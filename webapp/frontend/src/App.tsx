import { useState } from "react";
import Globe from "./components/Globe/Globe";
import Sidebar from "./components/Sidebar/Sidebar";
import Pathfinder from "./components/Pathfinder/Pathfinder";
import RouteDetail from "./components/RouteDetail/RouteDetail";
import FlightPopup from "./components/FlightPopup/FlightPopup";

export default function App() {
  const [flightPopup, setFlightPopup] = useState<{
    origin: string;
    destination: string;
    airline: string;
  } | null>(null);

  return (
    <div className="h-screen flex overflow-hidden bg-[#0d1117]">
      {/* Left panel — controls + results, full height, scrollable */}
      <div className="w-[480px] shrink-0 h-full overflow-y-auto border-r border-[#21262d]">
        <div className="p-4 space-y-3">
          <Sidebar />
          <Pathfinder />
          <RouteDetail />
        </div>
      </div>

      {/* Map — fills remaining width, full height */}
      <div className="flex-1 h-full">
        <Globe
          onArcClick={(origin, destination, airline) =>
            setFlightPopup({ origin, destination, airline })
          }
        />
      </div>

      {flightPopup && (
        <FlightPopup
          origin={flightPopup.origin}
          destination={flightPopup.destination}
          airline={flightPopup.airline}
          onClose={() => setFlightPopup(null)}
        />
      )}
    </div>
  );
}
