.PHONY: install airflow-init airflow-up airflow-down dbt-seed-run exports app test clean help

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip3
AIRFLOW_UID := $(shell id -u)

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install Python dependencies
	$(PIP) install -r requirements.txt
	@echo "✓ Dependencies installed"

airflow-init: ## Initialize Airflow database and create admin user
	@echo "AIRFLOW_UID=$(AIRFLOW_UID)" > .env.airflow
	@cat .env >> .env.airflow || true
	docker compose --env-file .env.airflow up airflow-init
	@echo "✓ Airflow initialized. Default credentials: airflow/airflow"

airflow-up: ## Start Airflow services
	@echo "AIRFLOW_UID=$(AIRFLOW_UID)" > .env.airflow
	@cat .env >> .env.airflow || true
	docker compose --env-file .env.airflow up -d
	@echo "✓ Airflow running at http://localhost:8080"
	@echo "  Username: airflow"
	@echo "  Password: airflow"

airflow-down: ## Stop and remove Airflow services
	docker compose down -v
	@rm -f .env.airflow
	@echo "✓ Airflow services stopped"

dbt-seed-run: ## Run dbt seed, run, and test
	@if [ ! -f .env ]; then cp .env.example .env; fi
	cd dbt && export DBT_PROFILES_DIR=. && dbt seed
	cd dbt && export DBT_PROFILES_DIR=. && dbt run
	cd dbt && export DBT_PROFILES_DIR=. && dbt test
	@echo "✓ dbt models built and tested"

exports: ## Export metrics to CSV files
	$(PYTHON) scripts/export_metrics.py
	@echo "✓ Metrics exported to data/exports/"

app: ## Run Streamlit analytics app
	streamlit run app/streamlit_app.py --server.port $${APP_PORT:-8501}

test: ## Run all tests
	pytest tests/ -v
	@echo "✓ All tests passed"

clean: ## Clean generated files and caches
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf dbt/target dbt/logs
	rm -rf airflow/logs/*.log
	rm -rf .pytest_cache
	@echo "✓ Cleaned generated files"

# Development shortcuts
dev-setup: install airflow-init ## Complete development setup
	@echo "✓ Development environment ready"

run-pipeline: ## Run the complete pipeline locally (without Airflow)
	@if [ ! -f .env ]; then cp .env.example .env; fi
	$(PYTHON) scripts/fetch_538.py
	$(PYTHON) scripts/load_duckdb.py
	$(MAKE) dbt-seed-run
	$(PYTHON) scripts/export_metrics.py
	@echo "✓ Pipeline executed successfully"

# Docker management
docker-logs: ## Show Airflow logs
	docker compose logs -f

docker-shell: ## Open shell in Airflow webserver container
	docker compose exec airflow-webserver bash

# Data management
reset-data: ## Reset all data (WARNING: destructive)
	@read -p "Are you sure? This will delete all data. [y/N] " confirm && \
	if [ "$$confirm" = "y" ]; then \
		rm -rf data/raw/*.csv data/warehouse/*.duckdb data/exports/*.csv; \
		echo "✓ Data reset complete"; \
	else \
		echo "Cancelled"; \
	fi

# Environment management  
check-env: ## Check if .env file exists
	@if [ ! -f .env ]; then \
		echo "⚠️  No .env file found. Creating from template..."; \
		cp .env.example .env; \
		echo "✓ Created .env file. Please review and update if needed."; \
	else \
		echo "✓ .env file exists"; \
	fi