import { useCallback, useEffect, useState } from "react";
import Globe from "./components/Globe/Globe";
import Sidebar from "./components/Sidebar/Sidebar";
import Pathfinder from "./components/Pathfinder/Pathfinder";
import RouteDetail from "./components/RouteDetail/RouteDetail";
import FlightPopup from "./components/FlightPopup/FlightPopup";
import ThemeToggle from "./components/ThemeToggle/ThemeToggle";
import { useThemeStore } from "./stores/themeStore";

export default function App() {
  const [flightPopup, setFlightPopup] = useState<{
    origin: string;
    destination: string;
    airline: string;
  } | null>(null);

  // Mirror theme into document root so the CSS overrides in index.css
  // (gated by [data-theme="light"]) take effect across the whole app.
  const theme = useThemeStore((s) => s.theme);
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  // Stable ref so Globe's useCallback/useMemo deps don't churn when App
  // re-renders for other reasons (theme toggle, popup open/close).
  const handleArcClick = useCallback(
    (origin: string, destination: string, airline: string) => {
      setFlightPopup({ origin, destination, airline });
    },
    [],
  );

  const closePopup = useCallback(() => setFlightPopup(null), []);

  return (
    <div className="relative w-full h-full">
      <Globe onArcClick={handleArcClick} />

      <ThemeToggle />

      {/* pointer-events-none lets gaps between panels (and empty space
          below) pass clicks through to the map. The panels themselves
          set pointer-events-auto on their own backdrop. */}
      <div className="absolute top-4 left-4 bottom-4 w-[420px] overflow-y-auto z-10 space-y-2 pr-1 pointer-events-none">
        <Sidebar />
        <Pathfinder />
      </div>

      <RouteDetail />

      {flightPopup && (
        <FlightPopup
          origin={flightPopup.origin}
          destination={flightPopup.destination}
          airline={flightPopup.airline}
          onClose={closePopup}
        />
      )}
    </div>
  );
}
