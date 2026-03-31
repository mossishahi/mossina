.PHONY: dev dev-db backend frontend scraper migrate makemigrations test clean

dev:
	docker compose up --build

dev-db:
	docker compose up db -d

backend:
	cd webapp/backend && uvicorn app.main:app --reload

frontend:
	cd webapp/frontend && npm run dev

scraper:
	docker compose run --rm scraper

migrate:
	cd webapp/backend && alembic upgrade head

makemigrations:
	cd webapp/backend && alembic revision --autogenerate -m "$(msg)"

test:
	pytest

clean:
	docker compose down -v
