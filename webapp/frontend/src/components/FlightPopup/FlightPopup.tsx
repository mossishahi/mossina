import { X, ExternalLink, Loader2, Plane } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { getFares } from "@/api/client";
import { useFilterStore } from "@/stores/filterStore";
import { AIRLINE_META } from "@/api/types";

interface Props {
  origin: string;
  destination: string;
  airline: string;
  onClose: () => void;
}

function getBookingUrl(airline: string, origin: string, dest: string): string {
  if (airline === "FR") {
    return `https://www.ryanair.com/gb/en/trip/flights/select?adults=1&origin=${origin}&destination=${dest}`;
  }
  if (airline === "W6") {
    return `https://wizzair.com/en-gb/flights/${origin.toLowerCase()}-${dest.toLowerCase()}`;
  }
  return "#";
}

export default function FlightPopup({
  origin,
  destination,
  airline,
  onClose,
}: Props) {
  const { dateFrom, dateTo } = useFilterStore();

  const { data, isLoading, isError } = useQuery({
    queryKey: ["fares", origin, destination, dateFrom, dateTo],
    queryFn: () =>
      getFares(origin, destination, {
        date_from: dateFrom || undefined,
        date_to: dateTo || undefined,
      }),
  });

  const meta = AIRLINE_META[airline];
  const fares = data?.fares ?? [];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-[#161b22] border border-[#30363d] rounded-xl w-full max-w-md mx-4 shadow-2xl">
        <div className="flex items-center justify-between p-4 border-b border-[#30363d]">
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <span className="text-base font-semibold text-white">
                {origin}
              </span>
              <Plane
                size={16}
                style={{ color: meta?.color || "#58a6ff" }}
              />
              <span className="text-base font-semibold text-white">
                {destination}
              </span>
            </div>
            {meta && (
              <span
                className="text-xs font-medium px-2 py-0.5 rounded-full"
                style={{
                  color: meta.color,
                  backgroundColor: `${meta.color}20`,
                }}
              >
                {meta.name}
              </span>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-[#8b949e] hover:text-white transition-colors"
          >
            <X size={18} />
          </button>
        </div>

        <div className="max-h-80 overflow-y-auto">
          {isLoading && (
            <div className="flex items-center justify-center py-12">
              <Loader2 size={24} className="animate-spin text-[#58a6ff]" />
            </div>
          )}

          {isError && (
            <div className="p-4 text-sm text-[#e5534b]">
              Failed to load fares.
            </div>
          )}

          {!isLoading && fares.length === 0 && !isError && (
            <div className="p-8 text-center text-sm text-[#8b949e]">
              No fares found for selected dates
            </div>
          )}

          {fares.map((fare, i) => (
            <div
              key={i}
              className="flex items-center justify-between px-4 py-3 border-b border-[#30363d]/50 hover:bg-[#0d1117]/50 transition-colors"
            >
              <div>
                <span className="text-sm text-[#c9d1d9]">{fare.date}</span>
              </div>
              <div className="flex items-center gap-3">
                <div className="text-right">
                  <span className="text-sm font-semibold text-[#3fb950]">
                    EUR {fare.price_eur.toFixed(2)}
                  </span>
                  {fare.currency !== "EUR" && (
                    <span className="text-xs text-[#8b949e] ml-1.5">
                      ({fare.currency} {fare.price.toFixed(2)})
                    </span>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>

        <div className="p-3 border-t border-[#30363d]">
          <a
            href={getBookingUrl(airline, origin, destination)}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center justify-center gap-2 w-full py-2 rounded-lg text-sm font-medium transition-colors"
            style={{
              backgroundColor: `${meta?.color || "#58a6ff"}20`,
              color: meta?.color || "#58a6ff",
              border: `1px solid ${meta?.color || "#58a6ff"}40`,
            }}
          >
            <ExternalLink size={14} />
            Book on {meta?.name || airline}
          </a>
        </div>
      </div>
    </div>
  );
}
