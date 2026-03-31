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
  date: string;
  price: number;
  currency: string;
  price_eur: number;
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
  date: string;
  price: number;
  currency: string;
  price_eur: number;
}

export interface PathResult {
  legs: PathLeg[];
  total_eur: number;
  cities: string[];
}

export interface SearchResponse {
  paths: PathResult[];
  elapsed_seconds: number;
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
