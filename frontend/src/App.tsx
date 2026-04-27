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
    <div className="relative w-full h-full">
      <Globe onArcClick={(origin, destination, airline) =>
        setFlightPopup({ origin, destination, airline })
      } />

      <div className="absolute top-4 left-4 bottom-4 w-[420px] overflow-y-auto z-10 space-y-2 pr-1">
        <Sidebar />
        <Pathfinder />
      </div>

      <RouteDetail />

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
