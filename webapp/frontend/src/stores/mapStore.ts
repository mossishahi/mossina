import { create } from "zustand";

interface MapState {
  selectedCities: Set<string>;
  activeAirlines: Set<string>;

  toggleCity: (iata: string) => void;
  toggleAirline: (code: string) => void;
  toggleCountry: (iataCodes: string[]) => void;
  clearSelection: () => void;
  setSelectedCities: (cities: string[]) => void;
}

export const useMapStore = create<MapState>((set) => ({
  selectedCities: new Set<string>(),
  activeAirlines: new Set<string>(["FR", "W6"]),

  toggleCity: (iata) =>
    set((state) => {
      const next = new Set(state.selectedCities);
      if (next.has(iata)) {
        next.delete(iata);
      } else {
        next.add(iata);
      }
      return { selectedCities: next };
    }),

  toggleAirline: (code) =>
    set((state) => {
      const next = new Set(state.activeAirlines);
      if (next.has(code)) {
        next.delete(code);
      } else {
        next.add(code);
      }
      return { activeAirlines: next };
    }),

  toggleCountry: (iataCodes) =>
    set((state) => {
      const next = new Set(state.selectedCities);
      const allSelected = iataCodes.every((c) => next.has(c));
      if (allSelected) {
        iataCodes.forEach((c) => next.delete(c));
      } else {
        iataCodes.forEach((c) => next.add(c));
      }
      return { selectedCities: next };
    }),

  clearSelection: () => set({ selectedCities: new Set<string>() }),

  setSelectedCities: (cities) =>
    set({ selectedCities: new Set<string>(cities) }),
}));
