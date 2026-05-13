import { useState, useMemo } from "react";
import { ChevronDown, ChevronRight, X, Plane, ExternalLink, Check } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { getFares } from "@/api/client";
import { useAirports } from "@/hooks/useAirports";
import { usePathStore, pathKey_ } from "@/stores/pathStore";
import type { SegmentSelection } from "@/stores/pathStore";
import { useFilterStore } from "@/stores/filterStore";
import { AIRLINE_META } from "@/api/types";
import type { PathResult, PathLeg } from "@/api/types";

export default function RouteDetail() {
  const selectedPaths = usePathStore((s) => s.selectedPaths);
  const togglePath = usePathStore((s) => s.togglePath);
  const clearPaths = usePathStore((s) => s.clearPaths);
  const segmentSelections = usePathStore((s) => s.segmentSelections);
  const minHoursPerStop = usePathStore((s) => s.minHoursPerStop);

  if (selectedPaths.length === 0) return null;

  return (
    // pointer-events-none lets the empty gaps between/below path cards
    // pass mouse + wheel events through to the map underneath. Every
    // interactive descendant (header + each PathPanel) re-enables them.
    // Scrolling of this overflow container still works because wheel
    // events from pointer-events-auto descendants bubble back up to it.
    <div className="absolute top-4 bottom-4 overflow-y-auto z-10 space-y-2 pr-1 pointer-events-none"
      style={{ left: "calc(420px + 24px)", width: "400px" }}
    >
      <div className="flex items-center justify-between px-1 mb-1 pointer-events-auto">
        <span className="text-xs font-semibold text-[#8b949e] uppercase tracking-wider">
          {selectedPaths.length} trip{selectedPaths.length > 1 ? "s" : ""} selected
        </span>
        <button
          onClick={() => clearPaths()}
          className="text-[10px] text-[#e5534b] hover:underline"
        >
          Clear all
        </button>
      </div>

      {selectedPaths.map((path) => {
        const pk = pathKey_(path);
        const sels = segmentSelections[pk] || [];
        const mh = minHoursPerStop[pk] || [];
        return (
          <PathPanel
            key={pk}
            path={path}
            pathKey={pk}
            selections={sels}
            minHours={mh}
            onRemove={() => togglePath(path)}
          />
        );
      })}
    </div>
  );
}

function PathPanel({
  path,
  pathKey,
  selections,
  minHours,
  onRemove,
}: {
  path: PathResult;
  pathKey: string;
  selections: (SegmentSelection | null)[];
  minHours: (number | null)[];
  onRemove: () => void;
}) {
  const autoSelectBestDates = usePathStore((s) => s.autoSelectBestDates);
  const [openSegments, setOpenSegments] = useState<Set<number>>(new Set());
  const [flashSegments, setFlashSegments] = useState(false);

  const { data: airports = [] } = useAirports();
  const nameMap = useMemo(() => {
    const m = new Map<string, string>();
    airports.forEach((a) => m.set(a.iata, a.city || a.name));
    return m;
  }, [airports]);

  const cityName = (iata: string) => nameMap.get(iata) || iata;

  const selectedTotal = useMemo(() => {
    let sum = 0;
    let allPicked = true;
    selections.forEach((s) => {
      if (s) sum += s.price_eur;
      else allPicked = false;
    });
    return allPicked ? sum : null;
  }, [selections]);

  const costLabel = selectedTotal != null
    ? `${selectedTotal.toFixed(2)}\u20AC`
    : path.total_cost_eur != null
      ? `~${path.total_cost_eur.toFixed(2)}\u20AC`
      : "--";

  function handleHighlightBest() {
    autoSelectBestDates(pathKey, path);
    const allIdxs = new Set(path.legs.map((_, i) => i));
    setOpenSegments(allIdxs);
    setFlashSegments(true);
    setTimeout(() => setFlashSegments(false), 1500);
  }

  return (
    <div className="bg-black/85 backdrop-blur-xl border border-[#30363d] rounded-xl overflow-hidden pointer-events-auto">
      <div className="flex items-center gap-2 px-3 py-2.5 border-b border-[#21262d]">
        <Plane size={13} className="text-[#58a6ff] shrink-0" />
        <div className="flex items-center gap-1 flex-1 min-w-0 overflow-x-auto whitespace-nowrap">
          {path.path.map((iata, i) => {
            const leg = i < path.legs.length ? path.legs[i] : null;
            const color = leg ? AIRLINE_META[leg.airline]?.color || "#8b949e" : "#8b949e";
            return (
              <span key={i} className="inline-flex items-center gap-0.5">
                <span className="text-[11px] text-[#c9d1d9] font-medium">{cityName(iata)}</span>
                {i < path.path.length - 1 && (
                  <span className="text-[11px] font-bold" style={{ color }}>{"\u2192"}</span>
                )}
              </span>
            );
          })}
        </div>
        <span
          onClick={(e) => {
            e.stopPropagation();
            handleHighlightBest();
          }}
          title="Click to highlight cheapest combination"
          className="text-xs font-bold shrink-0 cursor-pointer text-[#58a6ff] hover:text-[#79c0ff]"
        >
          {costLabel}
        </span>
        <button onClick={onRemove} className="text-[#484f58] hover:text-[#e5534b] shrink-0">
          <X size={13} />
        </button>
      </div>

      <div>
        {path.legs.map((leg, i) => {
          const minHoursAtStop = minHours[i] || 0;
          let prevDate: string | null = null;
          if (i > 0 && selections[i - 1]) {
            const base = new Date(selections[i - 1]!.date + "T00:00:00");
            const offsetMs = minHoursAtStop * 60 * 60 * 1000;
            const shifted = new Date(base.getTime() + offsetMs);
            prevDate = shifted.toISOString().slice(0, 10);
          }
          return (
            <SegmentDrawer
              key={i}
              leg={leg}
              index={i}
              cityName={cityName}
              pathKey={pathKey}
              minDate={prevDate}
              selection={selections[i] || null}
              forceOpen={openSegments.has(i)}
              onToggleOpen={(open) => {
                setOpenSegments((prev) => {
                  const next = new Set(prev);
                  if (open) next.add(i); else next.delete(i);
                  return next;
                });
              }}
              flash={flashSegments}
            />
          );
        })}
      </div>
    </div>
  );
}

function SegmentDrawer({
  leg,
  index,
  cityName,
  pathKey,
  minDate,
  selection,
  forceOpen,
  onToggleOpen,
  flash,
}: {
  leg: PathLeg;
  index: number;
  cityName: (iata: string) => string;
  pathKey: string;
  minDate: string | null;
  selection: SegmentSelection | null;
  forceOpen: boolean;
  onToggleOpen: (open: boolean) => void;
  flash: boolean;
}) {
  const open = forceOpen;
  const { dateFrom, dateTo } = useFilterStore();
  const selectSegmentDate = usePathStore((s) => s.selectSegmentDate);
  const meta = AIRLINE_META[leg.airline];
  const color = meta?.color || "#8b949e";

  const effectiveDateFrom = minDate || dateFrom || undefined;

  const { data, isLoading } = useQuery({
    queryKey: ["fares", leg.origin, leg.destination, effectiveDateFrom, dateTo],
    queryFn: () =>
      getFares(leg.origin, leg.destination, {
        date_from: effectiveDateFrom,
        date_to: dateTo || undefined,
      }),
    enabled: open,
    staleTime: 60_000,
  });

  const fares = (data?.fares ?? []).filter((f) => Number(f.price) > 0);

  function handleSelectFare(fareDate: string, priceEur: number) {
    if (selection && selection.date === fareDate) {
      selectSegmentDate(pathKey, index, null);
    } else {
      selectSegmentDate(pathKey, index, { date: fareDate, price_eur: priceEur });
    }
  }

  const costDisplay = selection
    ? `${selection.price_eur.toFixed(2)}\u20AC`
    : leg.cost_eur != null
      ? `~${leg.cost_eur.toFixed(2)}\u20AC`
      : "--";

  return (
    <div className={index > 0 ? "border-t border-[#21262d]" : ""}>
      <button
        onClick={() => onToggleOpen(!open)}
        className="w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-[#161b22]/60 transition-colors"
      >
        {open ? (
          <ChevronDown size={11} className="text-[#484f58]" />
        ) : (
          <ChevronRight size={11} className="text-[#484f58]" />
        )}
        <span
          className="w-1.5 h-1.5 rounded-full shrink-0"
          style={{ backgroundColor: color }}
        />
        <span className="text-[11px] text-[#c9d1d9] flex-1">
          {cityName(leg.origin)} <span className="font-bold" style={{ color }}>{"\u2192"}</span> {cityName(leg.destination)}
        </span>
        {selection && (
          <span className="text-[10px] text-[#484f58]">
            {new Date(selection.date + "T00:00:00").toLocaleDateString("en-GB", { day: "numeric", month: "short" })}
          </span>
        )}
        <span className="text-[10px] font-medium" style={{ color }}>
          {meta?.name || leg.airline}
        </span>
        <span className={`text-[10px] font-semibold tabular-nums ${selection ? "text-[#3fb950]" : "text-[#8b949e]"}`}>
          {costDisplay}
        </span>
      </button>

      {open && (
        <div className="px-3 pb-2">
          {index > 0 && minDate && (
            <p className="text-[9px] text-[#484f58] mb-1">
              Showing flights from {new Date(minDate + "T00:00:00").toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" })}
            </p>
          )}
          {isLoading && (
            <p className="text-[10px] text-[#484f58] py-2">Loading flights...</p>
          )}
          {!isLoading && fares.length === 0 && (
            <p className="text-[10px] text-[#484f58] py-2">No flights found in this period</p>
          )}
          {fares.length > 0 && (
            <div className="space-y-0.5 max-h-48 overflow-y-auto">
              {fares.map((f, i) => {
                const dt = new Date(f.departure_date + "T00:00:00");
                const day = dt.toLocaleDateString("en-GB", { weekday: "short" });
                const dateLabel = dt.toLocaleDateString("en-GB", {
                  day: "numeric", month: "short",
                });
                const eurVal = f.price_eur != null ? Number(f.price_eur) : 0;
                const isSelected = selection?.date === f.departure_date && Math.abs(selection.price_eur - eurVal) < 0.01;
                const isFlashing = isSelected && flash;
                return (
                  <div
                    key={i}
                    onClick={() => handleSelectFare(f.departure_date, eurVal)}
                    className={`flex items-center gap-2 py-1 px-2 rounded text-[10px] group cursor-pointer transition-all ${
                      isSelected
                        ? "bg-[#58a6ff]/15 border border-[#58a6ff]/30"
                        : "hover:bg-[#0d1117] border border-transparent"
                    } ${isFlashing ? "best-fare-flash" : ""}`}
                  >
                    <Check size={10} className={`shrink-0 ${isSelected ? "text-[#58a6ff]" : "text-transparent"}`} />
                    <span className="text-[#484f58] w-6">{day}</span>
                    <span className="text-[#c9d1d9] w-14">{dateLabel}</span>
                    {f.departure_time && (
                      <span className="text-[#8b949e] tabular-nums">
                        {f.departure_time}
                        {f.arrival_time && <span className="text-[#484f58]">-{f.arrival_time}</span>}
                      </span>
                    )}
                    <span className="text-[#3fb950] font-semibold tabular-nums flex-1 text-right">
                      {eurVal > 0 ? `${eurVal.toFixed(2)}\u20AC` : "--"}
                    </span>
                    {eurVal > 0 && f.currency !== "EUR" && (
                      <span className="text-[#484f58]">
                        {Number(f.price).toFixed(0)} {f.currency}
                      </span>
                    )}
                    <a
                      href={buildBookingUrl(leg.airline, leg.origin, leg.destination, f.departure_date)}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      className="opacity-0 group-hover:opacity-100 text-[#58a6ff] transition-opacity"
                    >
                      <ExternalLink size={10} />
                    </a>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function buildBookingUrl(airline: string, from: string, to: string, date: string): string {
  if (airline === "W6") {
    return `https://wizzair.com/en-gb/booking/select-flight/${from}/${to}/${date}/null/1/0/0/0/null`;
  }
  return `https://www.ryanair.com/gb/en/trip/flights/select?adults=1&teens=0&children=0&infants=0&dateOut=${date}&isConnectedFlight=false&isReturn=false&originIata=${from}&destinationIata=${to}`;
}
