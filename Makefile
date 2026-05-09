.PHONY: dev dev-db backend frontend scraper migrate makemigrations test clean

dev:
	docker compose up --build

dev-db:
	docker compose up db -d

backend:
	cd backend && uvicorn app.main:app --reload

frontend:
	cd frontend && npm run dev

scraper:
	docker compose run --rm scraper

migrate:
	docker compose --profile migrate run --rm migrate

makemigrations:
	cd packages/db && alembic revision --autogenerate -m "$(msg)"

test:
	pytest

clean:
	docker compose down -v
