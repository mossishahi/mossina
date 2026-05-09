import "maplibre-gl/dist/maplibre-gl.css";
import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { DeckGL } from "@deck.gl/react";
import { ArcLayer, ScatterplotLayer } from "@deck.gl/layers";
import { Map as MapLibreMap } from "react-map-gl/maplibre";
import { useAirports } from "@/hooks/useAirports";
import { useRoutes } from "@/hooks/useRoutes";
import { useMapStore } from "@/stores/mapStore";
import { usePathStore } from "@/stores/pathStore";
import { useTabStore } from "@/stores/tabStore";
import { useThemeStore } from "@/stores/themeStore";
import { AIRLINE_META } from "@/api/types";

interface Props {
  onArcClick: (origin: string, destination: string, airline: string) => void;
}

interface ArcDatum {
  startLat: number;
  startLng: number;
  endLat: number;
  endLng: number;
  color: [number, number, number, number];
  airline: string;
  origin: string;
  destination: string;
  isPath: boolean;
  // Vertical arc tilt: deck.gl's getHeight controls how high the arc bows.
  height: number;
}

interface PointDatum {
  lat: number;
  lng: number;
  iata: string;
  name: string;
  selected: boolean;
}

const STYLES: Record<"dark" | "light", string> = {
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
};

const INITIAL_VIEW_STATE = {
  longitude: 15,
  latitude: 48,
  zoom: 3.4,
  bearing: 0,
  pitch: 0,
};

// Convert "#RRGGBB" or "#RGB" to [r, g, b, a] for deck.gl color accessors.
function hexToRgba(hex: string, alpha = 220): [number, number, number, number] {
  let h = hex.replace("#", "");
  if (h.length === 3) h = h.split("").map((c) => c + c).join("");
  const r = parseInt(h.slice(0, 2), 16);
  const g = parseInt(h.slice(2, 4), 16);
  const b = parseInt(h.slice(4, 6), 16);
  return [r, g, b, alpha];
}

export default function Globe({ onArcClick }: Props) {
  const { data: airports = [] } = useAirports();
  const { data: routes = [] } = useRoutes();
  const selectedCities = useMapStore((s) => s.selectedCities);
  const toggleCity = useMapStore((s) => s.toggleCity);
  const activeTab = useTabStore((s) => s.activeTab);
  const searchActive = usePathStore((s) => s.searchActive);
  const allTaggedPaths = usePathStore((s) => s.selectedPaths);
  const theme = useThemeStore((s) => s.theme);

  const [viewState, setViewState] = useState(INITIAL_VIEW_STATE);
  const lastFitSig = useRef<string>("");

  const selectedPaths = useMemo(
    () => allTaggedPaths.filter((tp) => tp.tab === activeTab).map((tp) => tp.result),
    [allTaggedPaths, activeTab],
  );

  const airportMap = useMemo(() => {
    const m = new Map<string, (typeof airports)[0]>();
    airports.forEach((a) => m.set(a.iata, a));
    return m;
  }, [airports]);

  // Auto-fit camera to selected paths (replaces the old globe pointOfView call).
  useEffect(() => {
    if (selectedPaths.length === 0) {
      lastFitSig.current = "";
      return;
    }
    const sig = selectedPaths
      .map((p) => p.path.join(">"))
      .sort()
      .join("|");
    if (sig === lastFitSig.current) return;
    lastFitSig.current = sig;

    const lats: number[] = [];
    const lons: number[] = [];
    selectedPaths.forEach((p) => {
      p.path.forEach((iata) => {
        const ap = airportMap.get(iata);
        if (ap) {
          lats.push(ap.lat);
          lons.push(ap.lon);
        }
      });
    });
    if (lats.length === 0) return;

    const minLat = Math.min(...lats);
    const maxLat = Math.max(...lats);
    const minLon = Math.min(...lons);
    const maxLon = Math.max(...lons);
    const midLat = (minLat + maxLat) / 2;
    const midLon = (minLon + maxLon) / 2;
    const span = Math.max(maxLat - minLat, (maxLon - minLon) * 0.7);
    // Rough zoom heuristic: span 1 deg -> z=8, span 30 deg -> z=4, span 60 deg -> z=3
    const zoom = Math.max(2.5, Math.min(7, 8 - Math.log2(Math.max(span, 1) + 1) * 1.6));

    setViewState((prev) => ({
      ...prev,
      longitude: midLon,
      latitude: midLat,
      zoom,
      transitionDuration: 800,
    }) as typeof prev);
  }, [selectedPaths, airportMap]);

  const points = useMemo<PointDatum[]>(
    () =>
      airports.map((a) => ({
        lat: a.lat,
        lng: a.lon,
        iata: a.iata,
        name: a.name,
        selected: selectedCities.has(a.iata),
      })),
    [airports, selectedCities],
  );

  // Routes touching any selected city -- with per-pair offset so multi-airline
  // pairs are visually separated.
  const selectionArcs = useMemo<ArcDatum[]>(() => {
    if (selectedCities.size === 0) return [];
    const filtered = routes.filter(
      (r) => selectedCities.has(r.origin) || selectedCities.has(r.destination),
    );
    const pairCount = new Map<string, number>();
    filtered.forEach((r) => {
      const key = [r.origin, r.destination].sort().join("-");
      pairCount.set(key, (pairCount.get(key) || 0) + 1);
    });
    const pairIdx = new Map<string, number>();
    const out: ArcDatum[] = [];
    for (const r of filtered) {
      const o = airportMap.get(r.origin);
      const d = airportMap.get(r.destination);
      if (!o || !d) continue;
      const meta = AIRLINE_META[r.airline];
      const key = [r.origin, r.destination].sort().join("-");
      const multi = (pairCount.get(key) || 1) > 1;
      const idx = pairIdx.get(key) || 0;
      pairIdx.set(key, idx + 1);
      out.push({
        startLat: o.lat,
        startLng: o.lon,
        endLat: d.lat,
        endLng: d.lon,
        color: hexToRgba(meta?.color || "#58a6ff", 200),
        airline: r.airline,
        origin: r.origin,
        destination: r.destination,
        isPath: false,
        height: multi ? 0.25 + idx * 0.18 : 0.35,
      });
    }
    return out;
  }, [routes, selectedCities, airportMap]);

  const pathArcs = useMemo<ArcDatum[]>(() => {
    if (selectedPaths.length === 0) return [];
    const out: ArcDatum[] = [];
    selectedPaths.forEach((p) => {
      p.legs.forEach((leg, i) => {
        const o = airportMap.get(leg.origin);
        const d = airportMap.get(leg.destination);
        if (!o || !d) return;
        const meta = AIRLINE_META[leg.airline];
        out.push({
          startLat: o.lat,
          startLng: o.lon,
          endLat: d.lat,
          endLng: d.lon,
          color: hexToRgba(meta?.color || "#58a6ff", 240),
          airline: leg.airline,
          origin: leg.origin,
          destination: leg.destination,
          isPath: true,
          height: 0.6 + i * 0.18,
        });
      });
    });
    return out;
  }, [selectedPaths, airportMap]);

  // Display priority: selected-path arcs first, otherwise selection arcs,
  // otherwise nothing while a search is active.
  const arcs = useMemo<ArcDatum[]>(() => {
    if (pathArcs.length > 0) return pathArcs;
    if (searchActive) return [];
    return selectionArcs;
  }, [pathArcs, selectionArcs, searchActive]);

  const handlePointClick = useCallback(
    (info: { object?: PointDatum }) => {
      if (info.object) toggleCity(info.object.iata);
    },
    [toggleCity],
  );

  const handleArcClick = useCallback(
    (info: { object?: ArcDatum }) => {
      const a = info.object;
      if (a) onArcClick(a.origin, a.destination, a.airline);
    },
    [onArcClick],
  );

  const layers = useMemo(
    () => [
      new ArcLayer<ArcDatum>({
        id: "arcs",
        data: arcs,
        getSourcePosition: (d) => [d.startLng, d.startLat],
        getTargetPosition: (d) => [d.endLng, d.endLat],
        getSourceColor: (d) => d.color,
        getTargetColor: (d) => d.color,
        getHeight: (d) => d.height,
        getWidth: (d) => (d.isPath ? 2.6 : 1.4),
        widthUnits: "pixels",
        greatCircle: false,
        pickable: true,
        onClick: handleArcClick,
        updateTriggers: {
          getSourceColor: theme,
          getTargetColor: theme,
        },
      }),
      new ScatterplotLayer<PointDatum>({
        id: "airports",
        data: points,
        getPosition: (d) => [d.lng, d.lat],
        getRadius: (d) => (d.selected ? 5 : 3),
        radiusUnits: "pixels",
        getFillColor: (d) =>
          d.selected
            ? [88, 166, 255, 255]
            : theme === "dark"
              ? [180, 190, 200, 220]
              : [60, 70, 90, 220],
        getLineColor: (d) =>
          d.selected
            ? [255, 255, 255, 255]
            : theme === "dark"
              ? [20, 25, 35, 220]
              : [255, 255, 255, 220],
        lineWidthUnits: "pixels",
        getLineWidth: 1,
        stroked: true,
        pickable: true,
        onClick: handlePointClick,
        updateTriggers: {
          getFillColor: theme,
          getLineColor: theme,
        },
      }),
    ],
    [arcs, points, theme, handleArcClick, handlePointClick],
  );

  const getTooltip = useCallback((info: any) => {
    if (!info?.object) return null;
    const isArc = info.layer?.id === "arcs";
    if (isArc) {
      const a = info.object as ArcDatum;
      const meta = AIRLINE_META[a.airline];
      return {
        html: `<div><span style="color:${meta?.color || "#58a6ff"};font-weight:600">${meta?.name || a.airline}</span> &nbsp; ${a.origin} → ${a.destination}</div>`,
        style: {
          background: theme === "dark" ? "#161b22" : "#ffffff",
          color: theme === "dark" ? "#c9d1d9" : "#24292f",
          border: `1px solid ${theme === "dark" ? "#30363d" : "#d0d7de"}`,
          borderRadius: "6px",
          padding: "4px 8px",
          fontSize: "12px",
          fontFamily: "Inter, system-ui, sans-serif",
        },
      };
    }
    const p = info.object as PointDatum;
    return {
      html: `<div><b>${p.iata}</b> ${p.name}</div>`,
      style: {
        background: theme === "dark" ? "#161b22" : "#ffffff",
        color: theme === "dark" ? "#c9d1d9" : "#24292f",
        border: `1px solid ${theme === "dark" ? "#30363d" : "#d0d7de"}`,
        borderRadius: "6px",
        padding: "4px 8px",
        fontSize: "12px",
        fontFamily: "Inter, system-ui, sans-serif",
      },
    };
  }, [theme]);

  return (
    <div className="absolute inset-0">
      <DeckGL
        viewState={viewState}
        onViewStateChange={(e: any) => setViewState(e.viewState)}
        controller={{ dragRotate: false, touchRotate: false }}
        layers={layers}
        getTooltip={getTooltip}
      >
        <MapLibreMap
          mapStyle={STYLES[theme]}
          attributionControl={false}
          reuseMaps
        />
      </DeckGL>
    </div>
  );
}
