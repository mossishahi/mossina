## mossina

Interactive flight route network explorer for Ryanair and Wizz Air.

### Quick start

```bash
# 1. Clone and set up
git clone <repo-url> && cd ryanair_scraper
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Place data (see "Using a shared data folder" below)

# 3. Run the app
python serve.py
```

The app opens automatically at `http://127.0.0.1:8080`.

### Running the app

```bash
python serve.py                          # default: http://127.0.0.1:8080
python serve.py --port 9000              # custom port
python serve.py --no-browser             # don't auto-open the browser
python serve.py --schedule 03:00         # daily auto-update at 3 AM
```

The page is rendered live from the database on every request — no
static files to generate or keep in sync.

#### Updating prices

**Manual (from the UI):** Click **"Fetch latest prices"** in the Update
Prices panel.  Select which airlines and cities to update, then watch
real-time progress with ETA.  Hit **Stop** any time — partial data is
saved.

**Automatic (daily schedule):** Add `--schedule HH:MM` when starting
the server.  A background thread runs a full update at the specified
local time every day:

```bash
python serve.py --schedule 03:00         # update all airlines at 3 AM
```

**One-off CLI (for cron / systemd):** Run the updater as a standalone
script — it updates, logs progress, and exits:

```bash
python -m src.update_worker                          # all airlines, all routes
python -m src.update_worker --airlines FR             # Ryanair only
python -m src.update_worker --origins FMM,NUE         # only routes touching FMM/NUE
python -m src.update_worker --airlines W6 --origins NUE
```

Example crontab entry for a daily 3 AM update:

```cron
0 3 * * * cd /path/to/ryanair_scraper && .venv/bin/python -m src.update_worker >> logs/update.log 2>&1
```

### Using a shared `data` folder

The repository does **not** include the SQLite database in git. If
someone sends you a pre-populated `data` folder (containing
`flights.db`), you have two options:

- **Simplest**: drop the `data` folder directly inside the cloned repo
  so you end up with `ryanair_scraper/data/flights.db`.

- **Alternative location**: point the app to data stored elsewhere via
  environment variables:
  - **`MOSSINA_DATA_DIR`** — directory that contains `flights.db`
  - **`MOSSINA_DB_PATH`** — full path to `flights.db` (overrides
    `MOSSINA_DATA_DIR`)

  ```bash
  export MOSSINA_DATA_DIR=/path/to/shared/data
  python serve.py
  ```

### Scraping fresh data

If you want to build the database from scratch instead of using a
shared one:

```bash
python scrape.py                          # full scrape (all airlines)
python scrape.py --airline FR             # Ryanair only
python scrape.py --availability-only --airline FR --origins FMM,NUE
```

See `python scrape.py --help` for all options.
