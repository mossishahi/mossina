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
    <div className="h-screen flex flex-col overflow-hidden bg-[#0d1117]">
      {/* Controls row — scrollable, sits above the map */}
      <div className="flex gap-3 px-3 pt-3 pb-2 overflow-y-auto shrink-0 max-h-[55vh]">
        <div className="w-[420px] shrink-0 space-y-2 pb-1">
          <Sidebar />
          <Pathfinder />
        </div>
        <RouteDetail />
      </div>

      {/* Map — fills remaining height, full width */}
      <div className="flex-1 min-h-[350px] w-full">
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
