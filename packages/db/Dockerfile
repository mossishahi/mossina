FROM python:3.11-slim

WORKDIR /app

COPY packages/db .
RUN pip install --no-cache-dir -e . && \
    pip install --no-cache-dir alembic psycopg2-binary

CMD ["alembic", "upgrade", "head"]
