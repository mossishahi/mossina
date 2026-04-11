import { useEffect, useMemo } from "react";
import { MapContainer, TileLayer, CircleMarker, Polyline, Tooltip, useMap } from "react-leaflet";
import { LatLngBounds } from "leaflet";
import { useAirports } from "@/hooks/useAirports";
import { useRoutes } from "@/hooks/useRoutes";
import { useMapStore } from "@/stores/mapStore";
import { usePathStore } from "@/stores/pathStore";
import { useTabStore } from "@/stores/tabStore";
import { AIRLINE_META } from "@/api/types";
import type { PathResult } from "@/api/types";

interface Props {
  onArcClick: (origin: string, destination: string, airline: string) => void;
}

interface LineDatum {
  positions: [number, number][];
  color: string;
  airline: string;
  origin: string;
  destination: string;
}

function BoundsController({
  selectedPaths,
  airportMap,
}: {
  selectedPaths: PathResult[];
  airportMap: Map<string, { lat: number; lon: number }>;
}) {
  const map = useMap();

  useEffect(() => {
    if (selectedPaths.length === 0) return;
    const points: [number, number][] = [];
    selectedPaths.forEach((p) => {
      p.path.forEach((iata) => {
        const ap = airportMap.get(iata);
        if (ap) points.push([ap.lat, ap.lon]);
      });
    });
    if (points.length === 0) return;
    const bounds = new LatLngBounds(points);
    map.fitBounds(bounds, { padding: [80, 80], maxZoom: 7 });
  }, [selectedPaths, airportMap, map]);

  return null;
}

export default function Globe({ onArcClick }: Props) {
  const { data: airports = [] } = useAirports();
  const { data: routes = [] } = useRoutes();
  const originCities = useMapStore((s) => s.originCities);
  const destinationCities = useMapStore((s) => s.destinationCities);
  const activeTab = useTabStore((s) => s.activeTab);
  const searchActive = usePathStore((s) => s.searchActive);
  const allTaggedPaths = usePathStore((s) => s.selectedPaths);

  const selectedPaths = useMemo(
    () => allTaggedPaths.filter((tp) => tp.tab === activeTab).map((tp) => tp.result),
    [allTaggedPaths, activeTab],
  );

  const airportMap = useMemo(() => {
    const m = new Map<string, (typeof airports)[0]>();
    airports.forEach((a) => m.set(a.iata, a));
    return m;
  }, [airports]);

  const allSelected = useMemo(
    () => new Set([...originCities, ...destinationCities]),
    [originCities, destinationCities],
  );

  const routeLines = useMemo<LineDatum[]>(() => {
    if (allSelected.size === 0) return [];
    return routes
      .filter((r) => allSelected.has(r.origin) || allSelected.has(r.destination))
      .flatMap((r) => {
        const o = airportMap.get(r.origin);
        const d = airportMap.get(r.destination);
        if (!o || !d) return [];
        const meta = AIRLINE_META[r.airline];
        return [{
          positions: [[o.lat, o.lon], [d.lat, d.lon]] as [number, number][],
          color: meta?.color || "#58a6ff",
          airline: r.airline,
          origin: r.origin,
          destination: r.destination,
        }];
      });
  }, [routes, allSelected, airportMap]);

  const pathLines = useMemo<LineDatum[]>(() => {
    if (selectedPaths.length === 0) return [];
    return selectedPaths.flatMap((p) =>
      p.legs.flatMap((leg) => {
        const o = airportMap.get(leg.origin);
        const d = airportMap.get(leg.destination);
        if (!o || !d) return [];
        const meta = AIRLINE_META[leg.airline];
        return [{
          positions: [[o.lat, o.lon], [d.lat, d.lon]] as [number, number][],
          color: meta?.color || "#58a6ff",
          airline: leg.airline,
          origin: leg.origin,
          destination: leg.destination,
        }];
      }),
    );
  }, [selectedPaths, airportMap]);

  const activeLines = useMemo(() => {
    if (pathLines.length > 0) return pathLines;
    if (searchActive) return [];
    return routeLines;
  }, [routeLines, pathLines, searchActive]);

  return (
    <div className="w-full h-full">
      <MapContainer
        center={[50, 15]}
        zoom={4}
        style={{ height: "100%", width: "100%" }}
        zoomControl={false}
      >
        <TileLayer
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
          maxZoom={19}
        />

        <BoundsController selectedPaths={selectedPaths} airportMap={airportMap} />

        {activeLines.map((line, i) => (
          <Polyline
            key={i}
            positions={line.positions}
            pathOptions={{ color: line.color, weight: 1.5, opacity: 0.75 }}
            eventHandlers={{ click: () => onArcClick(line.origin, line.destination, line.airline) }}
          >
            <Tooltip sticky>
              <span style={{ color: line.color }}>
                {AIRLINE_META[line.airline]?.name || line.airline}
              </span>{" "}
              {line.origin} → {line.destination}
            </Tooltip>
          </Polyline>
        ))}

        {airports.map((a) => {
          const isOrigin = originCities.has(a.iata);
          const isDest = destinationCities.has(a.iata);
          const color = isOrigin ? "#58a6ff" : isDest ? "#3fb950" : "#8b949e";
          const radius = isOrigin || isDest ? 6 : 3;
          return (
            <CircleMarker
              key={a.iata}
              center={[a.lat, a.lon]}
              radius={radius}
              pathOptions={{ color, fillColor: color, fillOpacity: 1, weight: 0 }}
            >
              <Tooltip>
                <b>{a.iata}</b> {a.name}
                {isOrigin && <span style={{ color: "#58a6ff" }}> · origin</span>}
                {isDest && <span style={{ color: "#3fb950" }}> · destination</span>}
              </Tooltip>
            </CircleMarker>
          );
        })}
      </MapContainer>
    </div>
  );
}
