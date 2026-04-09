# Mossina

Interactive multi-airline flight route explorer and trip planner. Search for the cheapest multi-hop paths and round-trip cycles across Ryanair and Wizz Air networks, with sequential date-aware pricing and real-time fare lookups.

## Architecture

The project is split into three independent services sharing a PostgreSQL database:

```
scraper/          Standalone data ingestion (Ryanair + Wizz Air APIs)
webapp/backend/   FastAPI REST API (async, SQLAlchemy 2.0, Alembic)
webapp/frontend/  React 18 + TypeScript + Vite + Tailwind + react-globe.gl
```

## Quick Start (Local Development)

### Prerequisites

- Docker (for PostgreSQL)
- Python 3.11+
- Node.js 20+

### 1. Start PostgreSQL

```bash
cp .env.example .env
docker compose up db -d
```

### 2. Run database migrations

```bash
cd webapp/backend
pip install -r requirements.txt
DATABASE_URL_SYNC="postgresql+psycopg2://mossina:mossina@localhost:5432/mossina" \
  alembic upgrade head
```

### 3. Migrate existing data (optional)

If you have an existing SQLite `data/flights.db`:

```bash
python scripts/migrate_sqlite_to_pg.py
```

### 4. Start the backend

```bash
cd webapp/backend
DATABASE_URL="postgresql+asyncpg://mossina:mossina@localhost:5432/mossina" \
CORS_ORIGINS='["http://localhost:3000","http://localhost:5173"]' \
  uvicorn app.main:app --reload --port 8000
```

### 5. Start the frontend

```bash
cd webapp/frontend
npm install
npm run dev
```

Open **http://localhost:5173**

### Quick commands (Makefile)

```bash
make dev-db       # Start PostgreSQL
make migrate      # Run Alembic migrations
make backend      # Start FastAPI (port 8000)
make frontend     # Start Vite dev server (port 5173)
make scraper      # Run scraper (Docker, on-demand)
make clean        # Tear down everything
```

## Features

### Interactive Globe
- 3D globe visualization with country borders
- Airport nodes colored by selection state
- Route arcs colored by airline (Ryanair blue, Wizz Air magenta)
- Double-click to zoom, auto-rotate when idle
- Selected trip arcs highlighted on the globe

### Route Finder
- **Paths**: find cheapest A-to-B routes with up to N hops
- **Cycles**: find cheapest round trips returning to origin
- Sequential date-aware pricing (each leg departs after the previous one + configurable min stay)
- Results grouped by hop count, sorted by total EUR cost
- Hop filters: constrain intermediate cities and minimum stay per stop

### Trip Planner (Route Detail Panel)
- Select any path/cycle to open it as a trip in the detail column
- Each segment expands to show available flights with departure/arrival times
- Click the price to auto-highlight the cheapest valid date combination
- Date constraints cascade: picking a date for segment 1 filters segment 2 to show only later dates
- Direct booking links to Ryanair and Wizz Air websites

### Search & Filter
- Fuzzy search for airports, cities, and countries
- Airline toggle (Ryanair / Wizz Air)
- Date range filter with calendar picker (past dates blocked)
- "Only selected cities" post-search filter
- Reset button to clear all filters at once

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/airports` | List all airports |
| GET | `/api/airports/search?q=` | Fuzzy search |
| GET | `/api/routes` | All routes |
| GET | `/api/fares/{from}/{to}` | Fares with flight times |
| POST | `/api/search/paths` | Path search (DFS + DP pricing) |
| POST | `/api/search/cycles` | Cycle search |
| GET | `/api/airlines` | Airline metadata |
| POST | `/api/exchange-rates/refresh` | Refresh EUR rates |

## Scraper

The scraper is a separate service that populates the database.

```bash
# Run via Docker
docker compose run --rm scraper

# Or directly
cd scraper
pip install -r requirements.txt
python cli.py                          # all airlines
python cli.py --airline FR             # Ryanair only
python cli.py --airline W6             # Wizz Air only
python cli.py --airline W6 --workers 4 # parallel Wizz Air sessions
```

### Wizz Air + ScrapFly

Wizz Air's API is behind bot protection. On servers, set `SCRAPFLY_API_KEY` to route requests through ScrapFly:

```bash
export SCRAPFLY_API_KEY="your-key"
python cli.py --airline W6
```

From residential IPs (laptops), direct access works without ScrapFly.

## Docker Compose (Production)

```bash
docker compose up -d           # PostgreSQL + backend + frontend (nginx)
docker compose run --rm scraper  # One-off scrape
```

Services:
- `db`: PostgreSQL 16 (port 5432, 256MB shared memory)
- `backend`: FastAPI (port 8000)
- `frontend`: Vite build served by nginx (port 3000)
- `scraper`: on-demand data ingestion (profile: scraper)

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Database | PostgreSQL 16 |
| Backend | FastAPI, SQLAlchemy 2.0 (async), Alembic, Pydantic v2 |
| Frontend | React 18, TypeScript, Vite, Tailwind CSS |
| Globe | react-globe.gl, Three.js |
| State | Zustand, TanStack Query |
| Scraping | requests, ScrapFly (optional), ThreadPoolExecutor |
| Exchange rates | open.er-api.com (free, fetched on demand) |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://mossina:mossina@db:5432/mossina` | Async DB URL (backend) |
| `DATABASE_URL_SYNC` | `postgresql+psycopg2://mossina:mossina@db:5432/mossina` | Sync DB URL (Alembic, scraper) |
| `CORS_ORIGINS` | `["http://localhost:3000"]` | Allowed CORS origins |
| `SCRAPFLY_API_KEY` | (none) | ScrapFly key for Wizz Air scraping |
| `WIZZAIR_API_URL` | (auto-discovered) | Override Wizz Air API base URL |
