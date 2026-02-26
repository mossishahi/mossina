## mossina
This is our airline.

### Using a shared `data` folder

The repository does **not** include the SQLite database in git. If someone sends you a pre-populated `data` folder (containing `flights.db`), you have two options:

- **Simplest**: drop the `data` folder directly inside the cloned repo:
  - `ryanair_scraper/`  
    - `data/` (from your friend, includes `flights.db`)  
    - `src/`, `output/`, etc.
  - In this case, no extra configuration is needed.

- **Alternative location**: if you want to keep the data somewhere else (e.g. on an external drive), point the code to it via environment variables:
  - **`MOSSINA_DATA_DIR`**: directory that contains `flights.db`
  - **`MOSSINA_DB_PATH`**: full path to `flights.db` (overrides `MOSSINA_DATA_DIR`)
  - **`MOSSINA_OUTPUT_DIR`**: directory where the HTML visualisation will be written

Example:

```bash
export MOSSINA_DATA_DIR=/path/to/shared/data
export MOSSINA_OUTPUT_DIR=/path/to/output
```

### Generating the route network HTML

1. Create and activate a virtualenv (optional but recommended):
   - **macOS / Linux**
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Make sure `flights.db` is available (see the `data` section above).

4. From the project root, run:
   ```bash
   python -m src.viz.network_graph
   ```

5. Open the generated file (by default):
   - `output/route_network.html`
