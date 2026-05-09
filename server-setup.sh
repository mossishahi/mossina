#!/usr/bin/env bash
# =============================================================================
# server-setup.sh
# Run this ONCE on a fresh DigitalOcean Droplet to prepare it for Mossina.
#
# Usage:
#   ssh root@YOUR_DROPLET_IP "bash -s" < server-setup.sh
# =============================================================================

set -euo pipefail

MOSSINA_DIR="$HOME/mossina-dev"

echo "==> [1/3] Installing Docker..."
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

echo "==> [2/3] Creating app directory at $MOSSINA_DIR..."
mkdir -p "$MOSSINA_DIR"
echo "    Directory created ✓"

echo "==> [3/3] Creating log files..."
touch /var/log/mossina-scrape.log /var/log/mossina-viz.log
echo "    Log files created ✓"

echo ""
echo "====================================================="
echo " Mossina server setup complete!"
echo "====================================================="
echo ""
echo " Next steps:"
echo "   1. Create the .env file:"
echo "      nano $MOSSINA_DIR/.env"
echo ""
echo "   2. Update GitHub Actions secrets:"
echo "      DO_HOST  = $(curl -s ifconfig.me)"
echo "      DO_USER  = root"
echo "      DO_SSH_KEY = <your private key>"
echo ""
echo "   3. Push to 'master' branch to trigger first deploy."
echo ""
echo "   4. Watch logs:"
echo "      tail -f /var/log/mossina-scrape.log"
echo "====================================================="
