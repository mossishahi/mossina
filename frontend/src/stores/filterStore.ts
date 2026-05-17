import { create } from "zustand";

function toISODate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

const today = new Date();
const oneWeek = new Date(today);
oneWeek.setDate(today.getDate() + 7);

// Must stay <= MAX_GROUND_DISTANCE_KM on the backend (200). The runtime
// slider can pick any value in [0, MAX_GROUND_DISTANCE_KM]; 0 disables
// ground transfers entirely.
export const GROUND_DISTANCE_MAX_KM = 200;

interface FilterState {
  dateFrom: string | null;
  dateTo: string | null;
  intersectMode: boolean;
  groundDistanceKm: number;

  setDateRange: (from: string, to: string) => void;
  clearDateRange: () => void;
  setIntersectMode: (v: boolean) => void;
  setGroundDistanceKm: (v: number) => void;
}

export const useFilterStore = create<FilterState>((set) => ({
  dateFrom: toISODate(today),
  dateTo: toISODate(oneWeek),
  intersectMode: false,
  groundDistanceKm: 0,

  setDateRange: (from, to) => set({ dateFrom: from, dateTo: to }),
  clearDateRange: () => set({ dateFrom: null, dateTo: null }),
  setIntersectMode: (v) => set({ intersectMode: v }),
  setGroundDistanceKm: (v) =>
    set({
      groundDistanceKm: Math.max(0, Math.min(GROUND_DISTANCE_MAX_KM, Math.round(v))),
    }),
}));
