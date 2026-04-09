import axios from "axios";
import type {
  Airport,
  Route,
  RouteFares,
  SearchResponse,
  AirlineInfo,
  ExchangeRate,
  PathSearchRequest,
  CycleSearchRequest,
} from "./types";

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || "/api",
});

export async function getAirports(): Promise<Airport[]> {
  const { data } = await api.get<Airport[]>("/airports");
  return data;
}

export async function searchAirports(q: string): Promise<Airport[]> {
  const { data } = await api.get<{ airports: Airport[]; countries: unknown[] }>(
    "/airports/search",
    { params: { q } },
  );
  return data.airports;
}

export async function getRoutes(filters?: {
  airline?: string;
}): Promise<Route[]> {
  const { data } = await api.get<Route[]>("/routes", { params: filters });
  return data;
}

export async function getRouteDetail(
  from: string,
  to: string,
): Promise<Route> {
  const { data } = await api.get<Route>(`/routes/${from}/${to}`);
  return data;
}

export async function getFares(
  origin: string,
  destination: string,
  opts?: { date_from?: string; date_to?: string },
): Promise<RouteFares> {
  const { data } = await api.get<RouteFares>(
    `/fares/${origin}/${destination}`,
    { params: opts },
  );
  return data;
}

export async function searchPaths(
  body: PathSearchRequest,
): Promise<SearchResponse> {
  const { data } = await api.post<SearchResponse>("/search/paths", body);
  return data;
}

export async function searchCycles(
  body: CycleSearchRequest,
): Promise<SearchResponse> {
  const { data } = await api.post<SearchResponse>("/search/cycles", body);
  return data;
}

export async function getAirlines(): Promise<AirlineInfo[]> {
  const { data } = await api.get<AirlineInfo[]>("/airlines");
  return data;
}

export async function getExchangeRates(): Promise<ExchangeRate[]> {
  const { data } = await api.get<ExchangeRate[]>("/exchange-rates");
  return data;
}
