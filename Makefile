SHELL := /bin/bash

.PHONY: docker/build
docker/build:
	docker compose build

.PHONY: docker/up
docker/up:
	docker compose up -d && export $$(grep -v '^#' .env | xargs)

.PHONY: docker/down
docker/down:
	docker compose down

.PHONY: docker/restart
docker/restart:
	docker compose down && docker compose up -d

.PHONY: uv/flights
uv/flights:
	uv run python -m include flights \
		data/raw \
		--max-pages 1 \
		--execution-date 2025-12-24

.PHONY: uv/openflights
uv/openflights:
	uv run python -m include openflights \
		data/raw/openflights

.PHONY: dbt/run
dbt/run:
	@echo "Loading environment variables from .env..."
	@set -a && source .env && set +a && cd dbt_transform && uv run dbt run

.PHONY: dbt/test
dbt/test:
	@set -a && source .env && set +a && cd dbt_transform && uv run dbt test

.PHONY: dbt/debug
dbt/debug:
	@set -a && source .env && set +a && cd dbt_transform && uv run dbt debug

.PHONY: metabase/setup-dashboard
metabase/setup-dashboard:
	@echo "Setting up Metabase dashboard..."
	@echo "Usage: make metabase/setup-dashboard EMAIL=admin@example.com PASSWORD=yourpassword"
	@if [ -z "$(EMAIL)" ] || [ -z "$(PASSWORD)" ]; then \
		echo "Error: EMAIL and PASSWORD are required"; \
		echo "Example: make metabase/setup-dashboard EMAIL=admin@example.com PASSWORD=admin123"; \
		exit 1; \
	fi
	uv run python metabase/setup_dashboard.py \
		--url http://localhost:3000 \
		--email $(EMAIL) \
		--password $(PASSWORD)

.PHONY: test
test:
	uv run pytest tests/ spark_jobs/tests/ -v

.PHONY: lint
lint:
	uv run sqlfluff lint dbt_transform/models/

.PHONY: lint/fix
lint/fix:
	uv run sqlfluff fix dbt_transform/models/