SHELL := /bin/bash

.PHONY: docker/build
docker/build:
	docker compose build

.PHONY: docker/up
docker/up:
	docker compose up -d

.PHONY: docker/down
docker/down:
	docker compose down

.PHONY: docker/restart
docker/restart:
docker compose down && docker compose up -d

.PHONY: uv/api
uv/api:
	export $$(grep -v '^#' .env | xargs) && uv run python -m include.api data/raw 1 2025-12-24