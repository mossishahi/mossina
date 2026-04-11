import { create } from "zustand";

interface MapState {
  originCities: Set<string>;       // max 3
  destinationCities: Set<string>;
  activeAirlines: Set<string>;

  toggleOrigin: (iata: string) => void;
  toggleDestination: (iata: string) => void;
  toggleAirline: (code: string) => void;
  clearAll: () => void;
}

export const useMapStore = create<MapState>((set) => ({
  originCities: new Set<string>(),
  destinationCities: new Set<string>(),
  activeAirlines: new Set<string>(["FR", "W6"]),

  toggleOrigin: (iata) =>
    set((state) => {
      const next = new Set(state.originCities);
      if (next.has(iata)) {
        next.delete(iata);
      } else if (next.size < 3) {
        next.add(iata);
      }
      return { originCities: next };
    }),

  toggleDestination: (iata) =>
    set((state) => {
      const next = new Set(state.destinationCities);
      if (next.has(iata)) {
        next.delete(iata);
      } else {
        next.add(iata);
      }
      return { destinationCities: next };
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

  clearAll: () =>
    set({
      originCities: new Set<string>(),
      destinationCities: new Set<string>(),
    }),
}));
