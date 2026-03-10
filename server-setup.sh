#!/usr/bin/env bash
# =============================================================================
# scripts/server-setup.sh
# Run this ONCE on a fresh DigitalOcean Droplet to prepare it for Mossina.
#
# Usage:
#   chmod +x scripts/server-setup.sh
#   ssh root@YOUR_DROPLET_IP "bash -s" < scripts/server-setup.sh
# =============================================================================

set -euo pipefail

MOSSINA_DIR="/opt/mossina"
COMPOSE_FILE="$MOSSINA_DIR/docker-compose.yml"
GITHUB_REPOSITORY="${GITHUB_REPOSITORY:-your-org/mossina}"   # override if needed

echo "==> [1/5] Installing Docker..."
apt-get update -qq
apt-get install -y --no-install-recommends ca-certificates curl gnupg

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update -qq
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

systemctl enable --now docker
echo "    Docker installed ✓"

echo "==> [2/5] Creating app directory at $MOSSINA_DIR..."
mkdir -p "$MOSSINA_DIR"

echo "==> [3/5] Writing docker-compose.yml..."
cat > "$COMPOSE_FILE" << COMPOSE
services:
  scraper:
    image: ghcr.io/${GITHUB_REPOSITORY}:latest
    container_name: mossina_scraper
    restart: "no"
    command: ["python", "scrape.py"]
    volumes:
      - mossina_data:/app/data
      - mossina_output:/app/output
    environment:
      - PYTHONUNBUFFERED=1
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "5"

  visualizer:
    image: ghcr.io/${GITHUB_REPOSITORY}:latest
    container_name: mossina_visualizer
    restart: "no"
    command: ["python", "visualize.py"]
    volumes:
      - mossina_data:/app/data
      - mossina_output:/app/output
    environment:
      - PYTHONUNBUFFERED=1
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "5"

volumes:
  mossina_data:
    driver: local
  mossina_output:
    driver: local
COMPOSE
echo "    docker-compose.yml written ✓"

echo "==> [4/5] Installing cron jobs..."
# Runs a full scrape every day at 02:00 AM server time.
# Runs the visualizer every day at 04:00 AM (after scrape completes).
# Logs go to /var/log/mossina-*.log
CRON_JOBS="0 2 * * * docker compose -f $COMPOSE_FILE run --rm scraper python scrape.py >> /var/log/mossina-scrape.log 2>&1
0 4 * * * docker compose -f $COMPOSE_FILE run --rm visualizer python visualize.py >> /var/log/mossina-viz.log 2>&1"

# Append to root's crontab (avoids wiping existing entries)
(crontab -l 2>/dev/null; echo "$CRON_JOBS") | crontab -
echo "    Cron jobs installed ✓"

echo "==> [5/5] Creating log files..."
touch /var/log/mossina-scrape.log /var/log/mossina-viz.log
echo "    Log files created ✓"

echo ""
echo "====================================================="
echo " Mossina server setup complete!"
echo "====================================================="
echo ""
echo " Next steps:"
echo "   1. Add your GitHub Actions secrets (see README):"
echo "      DO_HOST  = $(curl -s ifconfig.me)"
echo "      DO_USER  = root"
echo "      DO_SSH_KEY = <your private key>"
echo ""
echo "   2. Push to 'production' branch to trigger first deploy."
echo ""
echo "   3. After deploy, run manually to test:"
echo "      docker compose -f $COMPOSE_FILE run --rm scraper python scrape.py --airline FR"
echo ""
echo "   4. Watch logs:"
echo "      tail -f /var/log/mossina-scrape.log"
echo "====================================================="
