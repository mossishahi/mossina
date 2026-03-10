FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/mossishahi/mossina"
LABEL org.opencontainers.image.description="Flight data scraping and visualisation pipeline"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scrape.py visualize.py ./
COPY src/ ./src/
COPY scripts/ ./scripts/

RUN mkdir -p data output/reports

CMD ["python", "scrape.py"]