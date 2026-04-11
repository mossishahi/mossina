# Mossina — Project Reference

Mossina is a multi-airline flight route explorer and trip planner. It finds the cheapest multi-hop itineraries across **Ryanair (FR)** and **Wizz Air (W6)** networks and visualizes them on an interactive 3D globe.

---

## Architecture Overview

Three independent services share a single PostgreSQL 16 database.

```
PostgreSQL 16
      ▲                    ▲                        ▲
      │                    │                        │
 Scraper (Python)   Backend (FastAPI)       Frontend (React 18)
                       Port 8000              Port 5173 (dev)
                                              Port 8080 (prod/nginx)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 16-alpine |
| Backend | Python 3.11, FastAPI, SQLAlchemy 2.0 async, Pydantic v2, Alembic |
| Async driver | asyncpg |
| Sync driver (migrations) | psycopg2 |
| Frontend | React 18, TypeScript, Vite, Tailwind CSS v4 |
| State management | Zustand 5 |
| Data fetching | TanStack Query 5 |
| HTTP client | Axios |
| 3D globe | react-globe.gl (Three.js) |
| Containers | Docker Compose v2 |
| Web server (prod) | nginx |
| CI/CD | GitHub Actions → DigitalOcean droplet |

---

## Directory Structure

```
mossina/
├── webapp/
│   ├── backend/
│   │   ├── app/
│   │   │   ├── main.py              # FastAPI app, routers, CORS, lifespan
│   │   │   ├── config.py            # Pydantic Settings (.env)
│   │   │   ├── database.py          # AsyncEngine, AsyncSessionLocal, get_db
│   │   │   ├── models/              # SQLAlchemy ORM models
│   │   │   │   ├── airport.py       # airports, countries tables
│   │   │   │   ├── route.py         # routes table
│   │   │   │   ├── fare.py          # fares, schedules tables
│   │   │   │   └── exchange_rate.py # exchange_rates table
│   │   │   ├── schemas/             # Pydantic request/response schemas
│   │   │   │   ├── airport.py
│   │   │   │   ├── route.py
│   │   │   │   ├── fare.py
│   │   │   │   └── search.py        # PathSearchRequest, CycleSearchRequest, SearchResponse
│   │   │   ├── routers/             # FastAPI route handlers
│   │   │   │   ├── airports.py
│   │   │   │   ├── routes.py
│   │   │   │   ├── fares.py
│   │   │   │   ├── search.py
│   │   │   │   ├── airlines.py
│   │   │   │   └── exchange_rates.py
│   │   │   └── services/
│   │   │       ├── pathfinder.py    # Core DFS + DP search algorithm
│   │   │       ├── graph.py         # Route graph helpers
│   │   │       └── exchange_rates.py
│   │   └── alembic/
│   │       └── versions/
│   │           ├── 001_initial_schema.py
│   │           ├── 002_add_price_history.py
│   │           └── 003_add_route_history.py
│   └── frontend/
│       └── src/
│           ├── main.tsx             # React 18 entry, TanStack Query setup
│           ├── App.tsx              # Root layout (Globe + Sidebar + RouteDetail)
│           ├── api/
│           │   ├── client.ts        # Axios API functions
│           │   └── types.ts         # TypeScript interfaces matching backend schemas
│           ├── stores/
│           │   ├── mapStore.ts      # Selected cities, active airlines
│           │   ├── pathStore.ts     # Selected paths, per-leg date selections
│           │   ├── filterStore.ts   # Date range, intersect mode
│           │   └── tabStore.ts      # "paths" | "cycles" active tab
│           ├── hooks/
│           │   ├── useAirports.ts
│           │   ├── useRoutes.ts
│           │   └── useSearch.ts     # useMutation for path/cycle searches
│           └── components/
│               ├── Globe/
│               ├── Sidebar/         # AirlineFilter, TimeFrame, CountryTree
│               ├── Pathfinder/      # Search controls, hop/leg filters, results
│               ├── RouteDetail/     # Right panel, per-segment fare selection
│               ├── FlightPopup/     # Modal on arc click
│               └── TabBar/
└── scraper/
    ├── cli.py                       # Entry point, --airline FR|W6, --loop
    └── src/scrapers/
        ├── ryanair/                 # airports, routes, fares, schedules
        └── wizzair/                 # airports, routes, fares (via schedules endpoint)
```

---

## Database Schema

| Table | Primary Key | Purpose |
|---|---|---|
| `countries` | `code` (2-char ISO) | Country metadata and currency |
| `airports` | `iata_code` (3-char) | Airport lat/lon, city, timezone |
| `routes` | `(origin, destination, airline)` | Known O-D pairs |
| `fares` | `id` | Price, departure date, currency |
| `schedules` | `id` | Flight departure/arrival times (FR only) |
| `exchange_rates` | `currency` (3-char) | Rate to EUR, cached from open.er-api.com |
| `price_history` | `id` | Snapshot of fares before each scrape |
| `route_history` | `id` | Per-route fare availability snapshots |

**Critical index:** `fares(origin, destination, departure_date, airline)` — all path searches hit this.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/health` | Health check |
| GET | `/api/airports` | All airports, optional `?airline=FR` filter |
| GET | `/api/airports/search?q=` | Fuzzy search airports, cities, countries |
| GET | `/api/routes` | All routes with bookable fares |
| GET | `/api/routes/{origin}/{destination}` | Routes for a specific O-D pair |
| GET | `/api/fares/{origin}/{destination}` | All fares with flight times |
| GET | `/api/fares/{origin}/{destination}/cheapest` | Cheapest fare per date |
| POST | `/api/search/paths` | Find cheapest multi-hop A→B paths |
| POST | `/api/search/cycles` | Find cheapest round-trip cycles |
| GET | `/api/airlines` | Airline metadata + last scrape time |
| GET | `/api/airlines/{code}/stats` | Route count and airport count |
| GET | `/api/exchange-rates` | Cached exchange rates |
| POST | `/api/exchange-rates/refresh` | Fetch fresh rates from external API |

---

## Core Algorithm: Path/Cycle Search

**File:** `webapp/backend/app/services/pathfinder.py`

### Search request shape (simplified)

```python
PathSearchRequest:
  origins: list[str]           # IATA codes
  destinations: list[str]
  max_hops: int                # 1–8
  date_from: str               # YYYY-MM-DD
  date_to: str
  only_selected: bool          # restrict to selected cities only
  airline: str | None          # "FR" | "W6" | None (both)
  hop_filters: list[HopConstraint]   # per-position city include/exclude, min/max stay days
  leg_filters: list[LegConstraint]   # per-edge airline filter

HopConstraint:
  min_stay_days: int | None
  max_stay_days: int | None
  include_cities: list[str]
  exclude_cities: list[str]

CycleSearchRequest: same minus destinations
```

### How it works

1. **Graph load** — `fetch_filtered_route_edges()` returns only (origin, destination, airline) tuples that have at least one fare in the requested date range.
2. **Adjacency map** — built from those edges.
3. **Fare load** — `_load_leg_fares_by_date()` batch-loads all fares for the candidate edges, converts to EUR, returns `dict[(origin, destination)] → sorted list of (date, eur_price)`.
4. **DFS** — `dfs_path()` / `dfs_cycle()` explore up to `max_hops` deep, checking hop/leg constraints at each step.
5. **DP pricing** — `_sequential_best_cost()` computes the minimum total cost respecting sequential date constraints (each leg departs after previous arrival + min stay days). Uses suffix-min arrays for O(log n) lookups.
6. **Time budget** — terminates after 10 seconds; partial results are returned.
7. **Deduplication** — paths deduplicated by ordered airport signature; cycles by sorted unique airport set.

---

## Frontend State Flow

```
User selects cities on Globe
        ↓
mapStore.selectedCities (Set<string>)
        ↓
Pathfinder reads: selectedCities, dateFrom/To, activeAirlines, maxHops, hopFilters, legFilters
        ↓
pathMutation.mutate(PathSearchRequest) → POST /api/search/paths
cycleMutation.mutate(CycleSearchRequest) → POST /api/search/cycles
        ↓
Results rendered in Pathfinder accordion (grouped by hop count, sorted by cost)
        ↓
User clicks a result row
        ↓
pathStore.togglePath(result, tab)
        ↓
RouteDetail (right panel) renders the path
User selects dates per segment (cascading: picking leg 1 clears legs 2+)
```

### Zustand stores summary

| Store | Key state | Key actions |
|---|---|---|
| `mapStore` | `selectedCities: Set<string>`, `activeAirlines: Set<string>` | `toggleCity`, `toggleCountry`, `toggleAirline`, `clearSelection` |
| `pathStore` | `selectedPaths: TaggedPath[]`, `segmentSelections` per path | `togglePath`, `selectSegmentDate` (cascades), `autoSelectBestDates` |
| `filterStore` | `dateFrom`, `dateTo`, `intersectMode` | `setDateRange`, `setIntersectMode` |
| `tabStore` | `activeTab: "paths" \| "cycles"` | `setActiveTab` |

---

## Scraper

**Entry:** `scraper/cli.py`

```bash
python cli.py                        # All airlines, one run
python cli.py --airline FR           # Ryanair only
python cli.py --airline W6           # Wizz Air only
python cli.py --loop --interval 60   # Loop every 60 min
```

**Per scrape run:**
1. Snapshot current fares → `price_history`
2. Scrape airports
3. Scrape routes
4. Scrape fares (ThreadPoolExecutor, parallel per route)
5. Scrape schedules (FR only)
6. Record route history
7. Assert >= 1 fare stored (exits with code 1 otherwise)

**Wizz Air:** Protected by bot detection. Uses ScrapFly proxy in production (`SCRAPFLY_API_KEY` env var).

---

## Environment Variables

| Variable | Used by | Description |
|---|---|---|
| `DATABASE_URL` | backend | `postgresql+asyncpg://...` |
| `DATABASE_URL_SYNC` | scraper, alembic | `postgresql+psycopg2://...` |
| `POSTGRES_USER/PASSWORD/DB` | docker db service | DB credentials |
| `VITE_API_URL` | frontend build | API base URL (default `/api`) |
| `CORS_ORIGINS` | backend | Allowed origins list |
| `SCRAPFLY_API_KEY` | scraper | Proxy for Wizz Air bot protection |
| `TELEGRAM_BOT_TOKEN` | CI/CD cron | Scraper notifications |
| `TELEGRAM_CHAT_ID` | CI/CD cron | Scraper notifications |

---

## Local Development

```bash
make dev-db          # Start PostgreSQL only
make migrate         # Apply Alembic migrations
make backend         # Run uvicorn locally (port 8000)
make frontend        # Run Vite dev server (port 5173)

make dev             # Full stack via docker compose --build
make scraper         # One-time scrape via docker compose run

make makemigrations  # alembic revision --autogenerate
make clean           # docker compose down -v
```

---

## Deployment

- **Trigger:** push to `production` branch
- **Target:** DigitalOcean droplet via GitHub Actions SSH
- **Process:** pull code → build images → restart containers → health check
- **Scheduled scrapes:**
  - 23:00 Berlin (21:00 UTC): Ryanair full scrape
  - 03:00 Berlin (01:00 UTC): Wizz Air scrape with ScrapFly
  - Both send Telegram success/failure notifications

---

## Key Design Decisions

**Sequential DP pricing** — Multi-leg trip costs are not computed by summing cheapest legs independently. The DP algorithm enforces that leg N departs after leg N-1 arrives, respecting min/max stay constraints. This gives correct prices for real itineraries.

**Cross-airline paths** — A single path can mix FR and W6 legs. The search picks the cheapest airline per edge.

**Time-budgeted DFS** — The search graph can be exponentially large. A 10-second hard budget ensures the API always responds; partial results are flagged with `is_partial: true`.

**Cascading date selection** — In RouteDetail, selecting a date for leg 1 clears all downstream leg selections. This guides users toward valid sequential date combinations.

**Airline color identity** — FR is `#0b4ea2` (Ryanair blue), W6 is `#e500a4` (Wizz Air magenta). These are used consistently across arcs, badges, and filter buttons.
