export interface PaginationMeta {
  page: number;
  per_page: number;
  total: number;
  total_pages: number;
}

export interface Airport {
  iata_code: string;
  name: string;
  city: string | null;
  country_code: string | null;
  country_name: string | null;
  latitude: number | null;
  longitude: number | null;
  timezone: string | null;
}

export interface AirportListResponse {
  airports: Airport[];
  meta?: PaginationMeta;
}

export interface ConnectedRoute {
  destination: Airport;
  airline: string;
  has_fares: boolean;
  min_price: number | null;
  currency: string | null;
}

export interface AirportDetail {
  airport: Airport;
  outbound_routes: ConnectedRoute[];
  inbound_route_count: number;
}

export interface AirportSearchResult {
  iata_code: string;
  name: string;
  city: string | null;
  country_code: string | null;
  country_name: string | null;
  match_field: string;
}

export interface Country {
  code: string;
  name: string;
  currency: string | null;
}

export interface CountryListResponse {
  countries: Country[];
}

export interface Airline {
  code: string;
  name: string;
}

export interface AirlineListResponse {
  airlines: Airline[];
}

export interface Route {
  id: number;
  origin: string;
  origin_name: string | null;
  origin_city: string | null;
  origin_country: string | null;
  destination: string;
  destination_name: string | null;
  destination_city: string | null;
  destination_country: string | null;
  airline: string;
  is_connecting: boolean;
  new_route: boolean;
  seasonal_route: boolean;
  last_seen: string | null;
  min_price: number | null;
  currency: string | null;
}

export interface RouteListResponse {
  routes: Route[];
  meta: PaginationMeta;
}

export interface Fare {
  id: number;
  origin: string;
  origin_name: string | null;
  destination: string;
  destination_name: string | null;
  airline: string;
  departure_date: string | null;
  arrival_date: string | null;
  price: number | null;
  currency: string | null;
  flight_number: string | null;
  scraped_at: string | null;
}

export interface FareListResponse {
  fares: Fare[];
  meta: PaginationMeta;
}

export interface GraphNode {
  iata_code: string;
  name: string;
  city: string | null;
  country_code: string | null;
  country_name: string | null;
  latitude: number | null;
  longitude: number | null;
  route_count: number;
}

export interface GraphEdge {
  origin: string;
  destination: string;
  airline: string;
  min_price: number | null;
  currency: string | null;
}

export interface GraphResponse {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface TableCount {
  table: string;
  count: number;
}

export interface AirlineStat {
  airline: string;
  route_count: number;
}

export interface StatsResponse {
  tables: TableCount[];
  airlines: AirlineStat[];
  last_updated: string | null;
  db_size_mb: number;
}
