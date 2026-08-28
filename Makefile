SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help init up airflow-up live sync capture failure-drill evidence test lint check down logs terraform-check

help:
	@echo "MobilityFlow Seoul road traffic operations"
	@echo "  make init            Copy .env.example when .env is absent"
	@echo "  make up              Start the local data platform"
	@echo "  make airflow-up      Start Airflow webserver and scheduler"
	@echo "  make live            Fetch Seoul live traffic and run the full Airflow pipeline"
	@echo "  make sync            Fetch the configured Seoul areas once"
	@echo "  make capture         Capture the running Control Room as PNG, MP4 and GIF"
	@echo "  make failure-drill   Prove backlog recovery, DLQ and idempotent replay"
	@echo "  make evidence        Export the latest operational run manifest"
	@echo "  make check           Run lint, unit tests, Compose and Terraform checks"
	@echo "  make down            Stop containers without deleting volumes"

init:
	@test -f .env || cp .env.example .env

up: init
	docker compose up -d --build postgres redpanda minio minio-init redpanda-init bronze-writer spark-runner api prometheus grafana

airflow-up: up
	docker compose up airflow-init
	docker compose up -d airflow-webserver airflow-scheduler

live: init
	bash scripts/run_live.sh

sync:
	curl --fail --silent --show-error \
		-X POST http://localhost:18000/ops/sources/seoul-citydata/sync \
		-H "X-Ops-Token: $${OPS_TOKEN:-local-ops-token}" \
		-H "Content-Type: application/json" \
		-d '{"areas": null}'

capture:
	uv run --extra dev python scripts/capture_ui.py

evidence:
	mkdir -p docs/evidence/generated
	docker compose run --rm -T producer mobility-evidence --base-url http://api:8000 --output - > docs/evidence/generated/run_evidence.json

failure-drill: init
	bash scripts/failure_drill.sh

test:
	uv run --extra dev pytest

lint:
	uv run --extra dev ruff check .

terraform-check:
	docker run --rm -v "$(CURDIR)/infra/terraform:/work" -w /work hashicorp/terraform:1.13.3 fmt -check -recursive
	docker run --rm -v "$(CURDIR)/infra/terraform:/work" -w /work hashicorp/terraform:1.13.3 init -backend=false
	docker run --rm -v "$(CURDIR)/infra/terraform:/work" -w /work hashicorp/terraform:1.13.3 validate

check: lint test terraform-check
	docker compose config --quiet

down:
	docker compose down

logs:
	docker compose logs -f --tail=200
