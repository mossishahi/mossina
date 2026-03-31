import { useRef, useEffect, useMemo, useCallback } from "react";
import GlobeGL from "react-globe.gl";
import { useAirports } from "@/hooks/useAirports";
import { useRoutes } from "@/hooks/useRoutes";
import { useMapStore } from "@/stores/mapStore";
import { AIRLINE_META } from "@/api/types";

interface Props {
  onArcClick: (origin: string, destination: string, airline: string) => void;
}

interface ArcDatum {
  startLat: number;
  startLng: number;
  endLat: number;
  endLng: number;
  color: string;
  airline: string;
  origin: string;
  destination: string;
}

interface PointDatum {
  lat: number;
  lng: number;
  iata: string;
  name: string;
  selected: boolean;
  color: string;
}

export default function Globe({ onArcClick }: Props) {
  const globeRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const { data: airports = [] } = useAirports();
  const { data: routes = [] } = useRoutes();
  const selectedCities = useMapStore((s) => s.selectedCities);
  const toggleCity = useMapStore((s) => s.toggleCity);

  useEffect(() => {
    if (globeRef.current) {
      const controls = globeRef.current.controls();
      if (controls) {
        controls.autoRotate = true;
        controls.autoRotateSpeed = 0.3;
      }
      globeRef.current.pointOfView({ lat: 48, lng: 15, altitude: 1.8 }, 0);
    }
  }, []);

  const airportMap = useMemo(() => {
    const m = new Map<string, (typeof airports)[0]>();
    airports.forEach((a) => m.set(a.iata, a));
    return m;
  }, [airports]);

  const points: PointDatum[] = useMemo(
    () =>
      airports.map((a) => ({
        lat: a.lat,
        lng: a.lon,
        iata: a.iata,
        name: a.name,
        selected: selectedCities.has(a.iata),
        color: selectedCities.has(a.iata) ? "#58a6ff" : "#8b949e",
      })),
    [airports, selectedCities],
  );

  const arcs: ArcDatum[] = useMemo(() => {
    if (selectedCities.size === 0) return [];
    return routes
      .filter(
        (r) => selectedCities.has(r.origin) || selectedCities.has(r.destination),
      )
      .map((r) => {
        const o = airportMap.get(r.origin);
        const d = airportMap.get(r.destination);
        if (!o || !d) return null;
        const meta = AIRLINE_META[r.airline];
        return {
          startLat: o.lat,
          startLng: o.lon,
          endLat: d.lat,
          endLng: d.lon,
          color: meta?.color || "#58a6ff",
          airline: r.airline,
          origin: r.origin,
          destination: r.destination,
        };
      })
      .filter(Boolean) as ArcDatum[];
  }, [routes, selectedCities, airportMap]);

  const handlePointClick = useCallback(
    (point: object) => {
      const p = point as PointDatum;
      toggleCity(p.iata);
    },
    [toggleCity],
  );

  const handleArcClick = useCallback(
    (arc: object) => {
      const a = arc as ArcDatum;
      onArcClick(a.origin, a.destination, a.airline);
    },
    [onArcClick],
  );

  const pointLabel = useCallback((point: object) => {
    const p = point as PointDatum;
    return `<div style="background:#161b22;border:1px solid #30363d;padding:4px 8px;border-radius:6px;font-size:12px;color:#c9d1d9;">
      <b>${p.iata}</b> ${p.name}
    </div>`;
  }, []);

  const arcLabel = useCallback((arc: object) => {
    const a = arc as ArcDatum;
    const meta = AIRLINE_META[a.airline];
    return `<div style="background:#161b22;border:1px solid #30363d;padding:4px 8px;border-radius:6px;font-size:12px;color:#c9d1d9;">
      <span style="color:${meta?.color || "#58a6ff"}">${meta?.name || a.airline}</span>
      ${a.origin} -> ${a.destination}
    </div>`;
  }, []);

  return (
    <div ref={containerRef} className="absolute inset-0">
      <GlobeGL
        ref={globeRef}
        globeImageUrl="//unpkg.com/three-globe/example/img/earth-night.jpg"
        backgroundImageUrl="//unpkg.com/three-globe/example/img/night-sky.png"
        backgroundColor="#0d1117"
        pointsData={points}
        pointLat="lat"
        pointLng="lng"
        pointRadius={(d) => ((d as PointDatum).selected ? 0.35 : 0.15)}
        pointColor="color"
        pointAltitude={(d) => ((d as PointDatum).selected ? 0.01 : 0.005)}
        pointLabel={pointLabel}
        onPointClick={handlePointClick}
        arcsData={arcs}
        arcStartLat="startLat"
        arcStartLng="startLng"
        arcEndLat="endLat"
        arcEndLng="endLng"
        arcColor="color"
        arcDashLength={0.4}
        arcDashGap={0.2}
        arcDashAnimateTime={1500}
        arcStroke={0.5}
        arcLabel={arcLabel}
        onArcClick={handleArcClick}
        atmosphereColor="#58a6ff"
        atmosphereAltitude={0.15}
      />
    </div>
  );
}
