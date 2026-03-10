FROM python:3.11-slim

# Metadata
LABEL org.opencontainers.image.source="https://github.com/YOUR_ORG/mossina"
LABEL org.opencontainers.image.description="Flight data scraping and visualisation pipeline"

WORKDIR /app

# Install system deps (curl for healthchecks, ca-certificates for HTTPS)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY scrape.py visualize.py ./
COPY src/ ./src/

# Create runtime directories (will be overridden by volumes in production)
RUN mkdir -p data output/reports

# Default command — overridden per use case in docker-compose / cron
CMD ["python", "scrape.py"]
