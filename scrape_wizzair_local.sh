#!/usr/bin/env bash
#
# Run a full Wizzair scrape from your laptop, then sync the database
# back to the server.
#
# Usage:
#   ./scrape_wizzair_local.sh                    # full scrape
#   ./scrape_wizzair_local.sh --schedules-only   # schedules only
#
# Prerequisites:
#   - SSH access to the server (ssh root@167.99.243.218)
#   - Python 3 with requirements installed (pip install -r requirements.txt)
#
set -euo pipefail

SERVER="root@167.99.243.218"
SSH_KEY="$HOME/.ssh/mossina"
REMOTE_DB="/var/lib/docker/volumes/mossina_mossina_data/_data/flights.db"
LOCAL_DB="data/flights.db"

echo "==> Downloading production database from server ..."
mkdir -p data
scp -i "$SSH_KEY" "$SERVER:$REMOTE_DB" "$LOCAL_DB"
echo "    Downloaded $(du -h "$LOCAL_DB" | cut -f1)"

echo ""
echo "==> Running Wizzair scrape (this may take 1-2 hours) ..."
python3 scrape.py --airline W6 "$@"

echo ""
echo "==> Uploading updated database to server ..."
scp -i "$SSH_KEY" "$LOCAL_DB" "$SERVER:$REMOTE_DB"
echo "    Done! Database synced to server."
