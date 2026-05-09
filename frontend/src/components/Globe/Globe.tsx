import { useRef, useEffect, useState, useMemo, useCallback } from "react";
import GlobeGL from "react-globe.gl";
import { useAirports } from "@/hooks/useAirports";
import { useRoutes } from "@/hooks/useRoutes";
import { useMapStore } from "@/stores/mapStore";
import { usePathStore } from "@/stores/pathStore";
import { useTabStore } from "@/stores/tabStore";
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
  alt?: number;
}

interface PointDatum {
  lat: number;
  lng: number;
  iata: string;
  name: string;
  selected: boolean;
  color: string;
}

const COUNTRIES_GEO_URL =
  "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

export default function Globe({ onArcClick }: Props) {
  const globeRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [countries, setCountries] = useState<object[]>([]);

  const { data: airports = [] } = useAirports();
  const { data: routes = [] } = useRoutes();
  const selectedCities = useMapStore((s) => s.selectedCities);
  const toggleCity = useMapStore((s) => s.toggleCity);
  const activeTab = useTabStore((s) => s.activeTab);
  const searchActive = usePathStore((s) => s.searchActive);
  const allTaggedPaths = usePathStore((s) => s.selectedPaths);
  const selectedPaths = useMemo(
    () => allTaggedPaths.filter((tp) => tp.tab === activeTab).map((tp) => tp.result),
    [allTaggedPaths, activeTab],
  );

  useEffect(() => {
    (async () => {
      try {
        const resp = await fetch(COUNTRIES_GEO_URL);
        const topo = await resp.json();
        const { feature } = await import("topojson-client");
        const geo = feature(topo, topo.objects.countries) as any;
        setCountries(geo.features);
      } catch { /* ignore */ }
    })();
  }, []);

  useEffect(() => {
    if (globeRef.current) {
      const controls = globeRef.current.controls();
      if (controls) {
        controls.autoRotate = true;
        controls.autoRotateSpeed = 0.3;
        controls.enableDamping = true;
      }
      globeRef.current.pointOfView({ lat: 48, lng: 15, altitude: 1.8 }, 0);

      const renderer = globeRef.current.renderer();
      if (renderer) {
        renderer.domElement.addEventListener("dblclick", (e: MouseEvent) => {
          e.preventDefault();
          const pov = globeRef.current.pointOfView();
          const newAlt = Math.max(0.3, pov.altitude * 0.5);
          globeRef.current.pointOfView({ altitude: newAlt }, 600);
        });
      }
    }
  }, []);

  useEffect(() => {
    if (globeRef.current) {
      const controls = globeRef.current.controls();
      if (controls) {
        controls.autoRotate = selectedCities.size === 0 && selectedPaths.length === 0;
      }
    }
  }, [selectedCities, selectedPaths]);

  const airportMap = useMemo(() => {
    const m = new Map<string, (typeof airports)[0]>();
    airports.forEach((a) => m.set(a.iata, a));
    return m;
  }, [airports]);

  useEffect(() => {
    if (!globeRef.current || selectedPaths.length === 0) return;
    const lats: number[] = [];
    const lons: number[] = [];
    selectedPaths.forEach((p) => {
      p.path.forEach((iata) => {
        const ap = airportMap.get(iata);
        if (ap) { lats.push(ap.lat); lons.push(ap.lon); }
      });
    });
    if (lats.length === 0) return;
    const midLat = lats.reduce((a, b) => a + b, 0) / lats.length;
    const midLon = lons.reduce((a, b) => a + b, 0) / lons.length;
    const latSpan = Math.max(...lats) - Math.min(...lats);
    const lonSpan = Math.max(...lons) - Math.min(...lons);
    const span = Math.max(latSpan, lonSpan);
    const alt = Math.max(0.8, Math.min(2.5, span / 40));
    globeRef.current.pointOfView({ lat: midLat, lng: midLon, altitude: alt }, 800);
  }, [selectedPaths, airportMap]);

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

    // Track which route pairs have multiple airlines for altitude offset
    const pairCount = new Map<string, number>();
    const filtered = routes.filter(
      (r) => selectedCities.has(r.origin) || selectedCities.has(r.destination),
    );
    filtered.forEach((r) => {
      const key = [r.origin, r.destination].sort().join("-");
      pairCount.set(key, (pairCount.get(key) || 0) + 1);
    });
    const pairIdx = new Map<string, number>();

    return filtered
      .map((r) => {
        const o = airportMap.get(r.origin);
        const d = airportMap.get(r.destination);
        if (!o || !d) return null;
        const meta = AIRLINE_META[r.airline];
        const key = [r.origin, r.destination].sort().join("-");
        const multi = (pairCount.get(key) || 1) > 1;
        const idx = pairIdx.get(key) || 0;
        pairIdx.set(key, idx + 1);
        return {
          startLat: o.lat,
          startLng: o.lon,
          endLat: d.lat,
          endLng: d.lon,
          color: meta?.color || "#58a6ff",
          airline: r.airline,
          origin: r.origin,
          destination: r.destination,
          alt: multi ? 0.003 + idx * 0.012 : undefined,
        };
      })
      .filter(Boolean) as ArcDatum[];
  }, [routes, selectedCities, airportMap]);

  const pathArcs: ArcDatum[] = useMemo(() => {
    if (selectedPaths.length === 0) return [];
    const result: ArcDatum[] = [];
    selectedPaths.forEach((p) => {
      p.legs.forEach((leg, i) => {
        const o = airportMap.get(leg.origin);
        const d = airportMap.get(leg.destination);
        if (!o || !d) return;
        const meta = AIRLINE_META[leg.airline];
        result.push({
          startLat: o.lat,
          startLng: o.lon,
          endLat: d.lat,
          endLng: d.lon,
          color: meta?.color || "#58a6ff",
          airline: leg.airline,
          origin: leg.origin,
          destination: leg.destination,
          alt: 0.015 + i * 0.008,
        });
      });
    });
    return result;
  }, [selectedPaths, airportMap]);

  const allArcs = useMemo(() => {
    if (pathArcs.length > 0) return pathArcs;
    if (searchActive) return [];
    return arcs;
  }, [arcs, pathArcs, searchActive]);

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
        arcsData={allArcs}
        arcStartLat="startLat"
        arcStartLng="startLng"
        arcEndLat="endLat"
        arcEndLng="endLng"
        arcColor="color"
        arcStroke={(d) => {
          const a = d as ArcDatum;
          if (pathArcs.length > 0 && a.alt === 0.02) return 0.4;
          return a.alt != null ? 0.15 : 0.2;
        }}
        arcAltitude={(d) => {
          const a = d as ArcDatum;
          return a.alt != null ? a.alt : null;
        }}
        arcAltitudeAutoScale={0.4}
        arcLabel={arcLabel}
        onArcClick={handleArcClick}
        arcCurveResolution={64}
        polygonsData={countries}
        polygonCapColor={() => "rgba(0,0,0,0)"}
        polygonSideColor={() => "rgba(0,0,0,0)"}
        polygonStrokeColor={() => "rgba(100,120,140,0.35)"}
        polygonAltitude={0.001}
        atmosphereColor="#58a6ff"
        atmosphereAltitude={0.15}
      />
    </div>
  );
}
