import { Car } from "lucide-react";
import { GROUND_DISTANCE_MAX_KM, useFilterStore } from "@/stores/filterStore";

/**
 * Slider for the max ground-transfer distance (km). Zero means
 * "no ground transfers" -- the default. Bounded by the precomputed
 * graph's cutoff (200 km) so we never query something we don't have.
 */
export default function GroundDistance() {
  const km = useFilterStore((s) => s.groundDistanceKm);
  const setKm = useFilterStore((s) => s.setGroundDistanceKm);

  const enabled = km > 0;

  return (
    <div className="flex items-center gap-2">
      <Car
        size={12}
        strokeWidth={2.2}
        className={enabled ? "text-[#58a6ff] shrink-0" : "text-[#484f58] shrink-0"}
      />
      <span className="text-[10px] text-[#8b949e] shrink-0 select-none">
        Ground transfers
      </span>
      <input
        type="range"
        min={0}
        max={GROUND_DISTANCE_MAX_KM}
        step={5}
        value={km}
        onChange={(e) => setKm(Number(e.target.value))}
        className="ground-slider flex-1 h-[3px] appearance-none rounded-full cursor-pointer outline-none"
        title={enabled ? `Allow transfers up to ${km} km` : "Disabled"}
      />
      <span
        className={`text-[10px] tabular-nums w-10 text-right select-none ${
          enabled ? "text-[#c9d1d9]" : "text-[#484f58]"
        }`}
      >
        {km > 0 ? `${km} km` : "off"}
      </span>
    </div>
  );
}
