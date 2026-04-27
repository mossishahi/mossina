FROM python:3.11-slim

WORKDIR /app

COPY packages/db /packages/db
COPY scraper/requirements.txt .
RUN pip install --no-cache-dir /packages/db && \
    pip install --no-cache-dir -r requirements.txt

COPY scraper/ .

CMD ["python", "cli.py"]
