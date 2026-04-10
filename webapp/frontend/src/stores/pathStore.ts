import { create } from "zustand";
import type { PathResult } from "@/api/types";
import type { TabId } from "@/stores/tabStore";

export interface SegmentSelection {
  date: string;
  price_eur: number;
}

export interface TaggedPath {
  result: PathResult;
  tab: TabId;
}

interface PathStore {
  selectedPaths: TaggedPath[];
  segmentSelections: Record<string, (SegmentSelection | null)[]>;
  minHoursPerStop: Record<string, (number | null)[]>;
  searchActive: boolean;
  setSearchActive: (v: boolean) => void;
  togglePath: (path: PathResult, tab: TabId) => void;
  updateSelectedResults: (newResults: PathResult[], tab: TabId) => void;
  clearPaths: (tab?: TabId) => void;
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

  updateSelectedResults: (newResults, tab) =>
    set((state) => {
      const resultMap = new Map<string, PathResult>();
      newResults.forEach((r) => resultMap.set(pathKey(r), r));
      const nextSels = { ...state.segmentSelections };
      const updatedPaths = state.selectedPaths.map((tp) => {
        if (tp.tab !== tab) return tp;
        const k = pathKey(tp.result);
        const updated = resultMap.get(k);
        if (updated) {
          delete nextSels[k];
          return { ...tp, result: updated };
        }
        return tp;
      });
      return { selectedPaths: updatedPaths, segmentSelections: nextSels };
    }),

  togglePath: (path, tab) =>
    set((state) => {
      const key = pathKey(path);
      const exists = state.selectedPaths.some(
        (tp) => pathKey(tp.result) === key && tp.tab === tab,
      );
      if (exists) {
        const next = { ...state.segmentSelections };
        delete next[key];
        return {
          selectedPaths: state.selectedPaths.filter(
            (tp) => !(pathKey(tp.result) === key && tp.tab === tab),
          ),
          segmentSelections: next,
        };
      }
      return {
        selectedPaths: [...state.selectedPaths, { result: path, tab }],
        segmentSelections: {
          ...state.segmentSelections,
          [key]: new Array(path.legs.length).fill(null),
        },
      };
    }),

  clearPaths: (tab) =>
    set((state) => {
      if (!tab) return { selectedPaths: [], segmentSelections: {} };
      const remaining = state.selectedPaths.filter((tp) => tp.tab !== tab);
      const removedKeys = new Set(
        state.selectedPaths
          .filter((tp) => tp.tab === tab)
          .map((tp) => pathKey(tp.result)),
      );
      const nextSels = { ...state.segmentSelections };
      removedKeys.forEach((k) => delete nextSels[k]);
      return { selectedPaths: remaining, segmentSelections: nextSels };
    }),

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
  const legs = p.legs.map((l) => `${l.origin}-${l.destination}:${l.airline}:${l.best_date || ""}`).join("|");
  return `${p.path.join(">")}::${legs}`;
}
