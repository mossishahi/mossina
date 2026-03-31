import { create } from "zustand";

function toISODate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

const today = new Date();
const oneWeek = new Date(today);
oneWeek.setDate(today.getDate() + 7);

interface FilterState {
  dateFrom: string | null;
  dateTo: string | null;
  intersectMode: boolean;

  setDateRange: (from: string, to: string) => void;
  clearDateRange: () => void;
  setIntersectMode: (v: boolean) => void;
}

export const useFilterStore = create<FilterState>((set) => ({
  dateFrom: toISODate(today),
  dateTo: toISODate(oneWeek),
  intersectMode: false,

  setDateRange: (from, to) => set({ dateFrom: from, dateTo: to }),
  clearDateRange: () => set({ dateFrom: null, dateTo: null }),
  setIntersectMode: (v) => set({ intersectMode: v }),
}));
