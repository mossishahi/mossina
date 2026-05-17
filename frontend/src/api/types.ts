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

export type LegKind = "flight" | "ground";

export interface PathLeg {
  origin: string;
  destination: string;
  // For flights: IATA airline code. For ground: "GROUND" sentinel.
  airline: string;
  // Defaults to "flight" when the backend response predates the field.
  kind?: LegKind;
  cost_eur: number | null;
  best_date: string | null;
  // Populated only when kind === "ground".
  ground_distance_km?: number | null;
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
  last_scraped: string | null;
}

export interface HopConstraint {
  min_stay_days?: number | null;
  max_stay_days?: number | null;
  include_cities?: string[] | null;
  exclude_cities?: string[] | null;
}

export interface LegConstraint {
  airline?: string | null;
}

export interface PathSearchRequest {
  origins: string[];
  destinations: string[];
  // Max cities visited (origin + intermediates + destination). A ground
  // transfer to a new city also consumes a stop slot.
  max_stops: number;
  date_from: string;
  date_to: string;
  only_selected?: boolean;
  airline?: string;
  hop_filters?: HopConstraint[] | null;
  leg_filters?: LegConstraint[] | null;
  // Max ground-transfer distance in km. Omit / null / 0 = no transfers.
  ground_distance_km?: number | null;
}

export interface CycleSearchRequest {
  origins: string[];
  max_stops: number;
  date_from: string;
  date_to: string;
  only_selected?: boolean;
  hop_filters?: HopConstraint[] | null;
  leg_filters?: LegConstraint[] | null;
  ground_distance_km?: number | null;
}

export interface ExchangeRate {
  currency: string;
  rate: number;
}

export const AIRLINE_META: Record<string, { name: string; color: string }> = {
  FR: { name: "Ryanair", color: "#0b4ea2" },
  W6: { name: "Wizz Air", color: "#e500a4" },
};
