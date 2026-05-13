import { create } from "zustand";
import type { PathResult } from "@/api/types";

export interface SegmentSelection {
  date: string;
  price_eur: number;
}

interface PathStore {
  selectedPaths: PathResult[];
  segmentSelections: Record<string, (SegmentSelection | null)[]>;
  minHoursPerStop: Record<string, (number | null)[]>;
  searchActive: boolean;
  setSearchActive: (v: boolean) => void;
  togglePath: (path: PathResult) => void;
  updateSelectedResults: (newResults: PathResult[]) => void;
  clearPaths: () => void;
  selectSegmentDate: (pathKey: string, segIdx: number, sel: SegmentSelection | null) => void;
  autoSelectBestDates: (pathKey: string, path: PathResult) => void;
  setMinDays: (pathKey: string, stops: (number | null)[]) => void;
}

function pathKey(p: PathResult): string {
  const legs = p.legs.map((l) => `${l.origin}-${l.destination}:${l.airline}`).join("|");
  return `${p.path.join(">")}::${legs}`;
}

export const usePathStore = create<PathStore>((set) => ({
  selectedPaths: [],
  segmentSelections: {},
  minHoursPerStop: {},
  searchActive: false,
  setSearchActive: (v) => set({ searchActive: v }),

  updateSelectedResults: (newResults) =>
    set((state) => {
      const resultMap = new Map<string, PathResult>();
      newResults.forEach((r) => resultMap.set(pathKey(r), r));
      const nextSels = { ...state.segmentSelections };
      const updatedPaths = state.selectedPaths.map((p) => {
        const k = pathKey(p);
        const updated = resultMap.get(k);
        if (updated) {
          delete nextSels[k];
          return updated;
        }
        return p;
      });
      return { selectedPaths: updatedPaths, segmentSelections: nextSels };
    }),

  togglePath: (path) =>
    set((state) => {
      const key = pathKey(path);
      const exists = state.selectedPaths.some((p) => pathKey(p) === key);
      if (exists) {
        const next = { ...state.segmentSelections };
        delete next[key];
        return {
          selectedPaths: state.selectedPaths.filter((p) => pathKey(p) !== key),
          segmentSelections: next,
        };
      }
      return {
        selectedPaths: [...state.selectedPaths, path],
        segmentSelections: {
          ...state.segmentSelections,
          [key]: new Array(path.legs.length).fill(null),
        },
      };
    }),

  clearPaths: () => set({ selectedPaths: [], segmentSelections: {} }),

  selectSegmentDate: (pk, segIdx, sel) =>
    set((state) => {
      const prev = state.segmentSelections[pk];
      if (!prev) return state;
      const next = [...prev];
      next[segIdx] = sel;
      for (let i = segIdx + 1; i < next.length; i++) {
        next[i] = null;
      }
      return {
        segmentSelections: { ...state.segmentSelections, [pk]: next },
      };
    }),

  autoSelectBestDates: (pk, path) =>
    set((state) => {
      if (path.is_partial) return state;
      const sels: (SegmentSelection | null)[] = path.legs.map((leg) =>
        leg.best_date && leg.cost_eur != null
          ? { date: leg.best_date, price_eur: leg.cost_eur }
          : null,
      );
      return {
        segmentSelections: { ...state.segmentSelections, [pk]: sels },
      };
    }),

  setMinDays: (pk, stops) =>
    set((state) => ({
      minHoursPerStop: { ...state.minHoursPerStop, [pk]: stops },
    })),
}));

export function pathKey_(p: PathResult): string {
  const legs = p.legs.map((l) => `${l.origin}-${l.destination}:${l.airline}`).join("|");
  return `${p.path.join(">")}::${legs}`;
}
