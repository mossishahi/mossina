export interface Airport {
  iata: string;
  name: string;
  city: string;
  country: string;
  country_code: string;
  lat: number;
  lon: number;
  airlines: string[];
}

export interface Country {
  code: string;
  name: string;
  airports: Airport[];
}

export interface Route {
  origin: string;
  destination: string;
  airline: string;
  origin_name?: string;
  destination_name?: string;
}

export interface Fare {
  departure_date: string;
  price: number;
  currency: string;
  price_eur: number | null;
  airline: string;
  flight_number: string | null;
  departure_time: string | null;
  arrival_time: string | null;
}

export interface RouteFares {
  origin: string;
  destination: string;
  airline: string;
  fares: Fare[];
}

export interface PathLeg {
  origin: string;
  destination: string;
  airline: string;
  cost_eur: number | null;
  best_date: string | null;
}

export interface PathResult {
  path: string[];
  legs: PathLeg[];
  total_cost_eur: number | null;
  is_partial: boolean;
}

export interface SearchResponse {
  results: PathResult[];
  count: number;
  search_time_ms: number;
}

export interface AirlineInfo {
  code: string;
  name: string;
  color: string;
}

export interface PathSearchRequest {
  origins: string[];
  destinations: string[];
  max_hops: number;
  date_from: string;
  date_to: string;
  only_selected?: boolean;
  airline?: string;
}

export interface CycleSearchRequest {
  origins: string[];
  max_hops: number;
  date_from: string;
  date_to: string;
  only_selected?: boolean;
}

export interface ExchangeRate {
  currency: string;
  rate: number;
}

export const AIRLINE_META: Record<string, { name: string; color: string }> = {
  FR: { name: "Ryanair", color: "#0b4ea2" },
  W6: { name: "Wizz Air", color: "#e500a4" },
};
