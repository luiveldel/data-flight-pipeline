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