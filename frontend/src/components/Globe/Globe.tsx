import "maplibre-gl/dist/maplibre-gl.css";
import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { DeckGL } from "@deck.gl/react";
import { ArcLayer, PathLayer, ScatterplotLayer } from "@deck.gl/layers";
import { PathStyleExtension } from "@deck.gl/extensions";
import { Map as MapLibreMap } from "react-map-gl/maplibre";
import { useAirports } from "@/hooks/useAirports";
import { useMapStore } from "@/stores/mapStore";
import { usePathStore } from "@/stores/pathStore";
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

// Ground transfers render as flat dashed lines via PathLayer; ArcLayer
// doesn't support a dashed style so we keep the two kinds in separate
// layers.
interface GroundEdgeDatum {
  path: [[number, number], [number, number]];
  origin: string;
  destination: string;
  distanceKm: number;
  color: [number, number, number, number];
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
  const selectedCities = useMapStore((s) => s.selectedCities);
  const toggleCity = useMapStore((s) => s.toggleCity);
  const selectedPaths = usePathStore((s) => s.selectedPaths);
  const theme = useThemeStore((s) => s.theme);

  const [viewState, setViewState] = useState(INITIAL_VIEW_STATE);
  const lastFitSig = useRef<string>("");

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

    // Snap to view with a clean object (no spread of prev). Spreading the
    // previous state could carry over transient deck.gl-internal fields
    // (transitionInterpolator, _interactionState, etc.) and leave the
    // controller in a partially-paused state.
    setViewState({
      longitude: midLon,
      latitude: midLat,
      zoom,
      bearing: 0,
      pitch: 0,
    });
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

  const { flightArcs, groundEdges } = useMemo(() => {
    const arcs: ArcDatum[] = [];
    const grounds: GroundEdgeDatum[] = [];
    if (selectedPaths.length === 0) return { flightArcs: arcs, groundEdges: grounds };
    selectedPaths.forEach((p) => {
      p.legs.forEach((leg, i) => {
        const o = airportMap.get(leg.origin);
        const d = airportMap.get(leg.destination);
        if (!o || !d) return;
        if (leg.kind === "ground") {
          grounds.push({
            path: [[o.lon, o.lat], [d.lon, d.lat]],
            origin: leg.origin,
            destination: leg.destination,
            distanceKm: leg.ground_distance_km ?? 0,
            // Neutral gray, theme-aware. Slightly translucent so it
            // reads as "auxiliary" next to the colored flight arcs.
            color: theme === "dark" ? [180, 190, 200, 220] : [88, 96, 110, 220],
          });
        } else {
          const meta = AIRLINE_META[leg.airline];
          arcs.push({
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
        }
      });
    });
    return { flightArcs: arcs, groundEdges: grounds };
  }, [selectedPaths, airportMap, theme]);

  // Arcs are only rendered for an explicit path selection from the result
  // list. Picking cities in the sidebar/tree no longer paints their routes
  // -- the selected city dots themselves indicate activation.
  const arcs = flightArcs;

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
      // Ground transfers as flat dashed lines. PathStyleExtension provides
      // getDashArray / dashJustified at runtime which the PathLayer's
      // static types don't expose, hence the cast.
      new PathLayer<GroundEdgeDatum>({
        id: "ground-edges",
        data: groundEdges,
        getPath: (d) => d.path,
        getColor: (d) => d.color,
        getWidth: 2,
        widthUnits: "pixels",
        extensions: [new PathStyleExtension({ dash: true })],
        pickable: false,
        updateTriggers: { getColor: theme },
        ...({ getDashArray: [4, 3], dashJustified: true } as any),
      }),
      new ScatterplotLayer<PointDatum>({
        id: "airports",
        data: points,
        getPosition: (d) => [d.lng, d.lat],
        getRadius: (d) => (d.selected ? 7 : 3),
        radiusUnits: "pixels",
        // Selected dots: theme-aware "active" color. Bright cyan-ish on the
        // dark basemap (#58a6ff), saturated GitHub blue on the light basemap
        // (#0969da) so they pop against either backdrop.
        getFillColor: (d) =>
          d.selected
            ? theme === "dark"
              ? [88, 166, 255, 255]
              : [9, 105, 218, 255]
            : theme === "dark"
              ? [180, 190, 200, 220]
              : [60, 70, 90, 220],
        getLineColor: (d) =>
          d.selected
            ? theme === "dark"
              ? [255, 255, 255, 255]
              : [255, 255, 255, 255]
            : theme === "dark"
              ? [20, 25, 35, 220]
              : [255, 255, 255, 220],
        lineWidthUnits: "pixels",
        getLineWidth: (d) => (d.selected ? 2 : 1),
        stroked: true,
        pickable: true,
        onClick: handlePointClick,
        updateTriggers: {
          getFillColor: theme,
          getLineColor: theme,
          getRadius: 0,
          getLineWidth: 0,
        },
      }),
    ],
    [arcs, groundEdges, points, theme, handleArcClick, handlePointClick],
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
