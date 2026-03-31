import { useState } from "react";
import Globe from "./components/Globe/Globe";
import Sidebar from "./components/Sidebar/Sidebar";
import FlightPopup from "./components/FlightPopup/FlightPopup";
import Pathfinder from "./components/Pathfinder/Pathfinder";

export default function App() {
  const [flightPopup, setFlightPopup] = useState<{
    origin: string;
    destination: string;
    airline: string;
  } | null>(null);

  const [showPathfinder, setShowPathfinder] = useState(false);

  return (
    <div className="relative w-full h-full">
      <Globe onArcClick={(origin, destination, airline) =>
        setFlightPopup({ origin, destination, airline })
      } />

      <Sidebar
        onOpenPathfinder={() => setShowPathfinder(true)}
      />

      {showPathfinder && (
        <Pathfinder onClose={() => setShowPathfinder(false)} />
      )}

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
