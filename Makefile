.PHONY: help setup install ingest transform dbt-deps dbt-run dbt-test pipeline dashboard test lint terraform-init terraform-plan bundle-validate docker-up docker-down clean

PYTHON ?= python3
export PYTHONPATH := $(PWD)/src

help: ## Show available Make targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Initialize .env from .env.example if not present
	@test -f .env || cp .env.example .env
	@echo ".env is ready."

install: setup ## Install Python dependencies
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install -e .

ingest: ## Download Citi Bike tripdata archives and ingest to Data Lake (S3 or local)
	$(PYTHON) -m citi_bike_pipeline.cli ingest

transform: ## Run Bronze -> Silver -> Gold Medallion transformations
	$(PYTHON) -m citi_bike_pipeline.cli transform

quality: ## Run data quality assertions on Silver and Gold tables
	$(PYTHON) -m citi_bike_pipeline.cli quality

pipeline: ## Run full end-to-end pipeline (Ingest -> Bronze -> Silver -> Gold -> Data Quality)
	$(PYTHON) -m citi_bike_pipeline.cli run-all

dbt-deps: ## Install dbt packages (dbt_utils)
	cd dbt_citi_bike && dbt deps --profiles-dir .

dbt-run: dbt-deps ## Run dbt models against configured target (databricks or local duckdb)
	cd dbt_citi_bike && dbt run --profiles-dir .

dbt-test: dbt-deps ## Run dbt data quality and schema tests
	cd dbt_citi_bike && dbt test --profiles-dir .

dashboard: ## Launch the interactive Streamlit analytics dashboard
	$(PYTHON) -m streamlit run dashboard/app.py --server.port=8501

test: ## Run unit and integration test suite with pytest
	$(PYTHON) -m pytest tests/ -v

lint: ## Run code quality checks with ruff
	$(PYTHON) -m ruff check src/ tests/ dashboard/

terraform-init: ## Initialize Terraform providers for AWS + Databricks
	cd terraform && terraform init -backend=false

terraform-validate: terraform-init ## Validate Terraform IaC configuration
	cd terraform && terraform validate

bundle-validate: ## Validate Databricks Asset Bundle workflow definitions
	databricks bundle validate

docker-up: setup ## Run end-to-end pipeline and launch Streamlit dashboard via Docker Compose
	docker compose up --build

docker-down: ## Stop Docker Compose services
	docker compose down

clean: ## Clean temporary build and cache artifacts
	rm -rf .pytest_cache .ruff_cache build dist src/*.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} +
