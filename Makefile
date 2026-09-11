# Two ways to run the same pipeline:
#   Docker  : make up        -> Airflow + JupyterLab in containers
#   No Docker: make up-local -> .venv with the same pinned requirements
#
# Both end at the same place: make reproduce / make reproduce-local.

SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
BACKFILL_DAYS ?= 30
COMPOSE := docker compose

.DEFAULT_GOAL := help

## ---------------------------------------------------------------- docker ---

.PHONY: up
up: .env ## Build and start Airflow (:8080) and JupyterLab (:8888)
	$(COMPOSE) up -d --build
	@echo
	@echo "Airflow    : http://localhost:8080  (user 'admin'; password in airflow_home/simple_auth_manager_passwords.json.generated)"
	@echo "JupyterLab : http://localhost:8888  (no token)"
	@echo "Next       : make reproduce"

.PHONY: airflow-ui
airflow-ui: ## Print the Airflow URL and where to find the generated password
	@echo "Airflow : http://localhost:8080"
	@echo "user    : admin"
	@echo "password: $$(cat airflow_home/simple_auth_manager_passwords.json.generated 2>/dev/null || echo '<starts with the stack; see airflow_home/simple_auth_manager_passwords.json.generated>')"

.PHONY: notebook
notebook: ## Print the JupyterLab URL (no token)
	@echo "JupyterLab: http://localhost:8888  (no token)"
	@echo "Open notebooks/walkthrough.ipynb"

.PHONY: dbt
dbt: ## Run dbt inside the container
	$(COMPOSE) run --rm jupyter python -m pipelines.cli dbt-run

.PHONY: down
down: ## Stop the stack (the warehouse file survives)
	$(COMPOSE) down

.PHONY: logs
logs: ## Tail container logs
	$(COMPOSE) logs -f

.PHONY: shell
shell: ## Open a shell in the pipeline image
	$(COMPOSE) run --rm jupyter bash

.PHONY: reproduce
reproduce: ## Run the whole pipeline by executing the walkthrough notebook (Docker)
	$(COMPOSE) run --rm -e PYTHON=python jupyter bash scripts/reproduce.sh

.PHONY: backfill
backfill: ## Backfill the last N logical dates with the CLI (BACKFILL_DAYS=30)
	$(PY) -m pipelines.cli backfill --days $(BACKFILL_DAYS)

.PHONY: airflow-backfill
airflow-backfill: ## Let Airflow itself backfill the last 30 logical dates
	$(COMPOSE) exec airflow airflow backfill create --dag-id weather_pipeline \
		--from-date $$(date -u -d '30 days ago' +%Y-%m-%d) --to-date $$(date -u +%Y-%m-%d)

## ----------------------------------------------------------------- local ---

.PHONY: up-local
up-local: .env ## Create .venv with the pinned runtime (no Docker required)
	bash scripts/bootstrap_local.sh

.PHONY: reproduce-local
reproduce-local: ## Run the whole pipeline by executing the walkthrough notebook (.venv)
	PYTHON=$(PY) bash scripts/reproduce.sh

.PHONY: jupyter-local
jupyter-local: ## Serve the notebook from .venv on :8888
	$(PY) -m jupyterlab --port 8888 --no-browser --ServerApp.token='' --ServerApp.root_dir=.

.PHONY: airflow-local
airflow-local: ## Run Airflow standalone from .venv on :8080
	AIRFLOW_HOME=$(PWD)/airflow_home \
	AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	$(VENV)/bin/airflow standalone

.PHONY: dag-test
dag-test: ## Execute the whole DAG once for a logical date (DATE=YYYY-MM-DD)
	AIRFLOW_HOME=$(PWD)/airflow_home \
	AIRFLOW__CORE__DAGS_FOLDER=$(PWD)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	$(VENV)/bin/airflow dags test weather_pipeline $(or $(DATE),$(shell date -u +%Y-%m-%d))

## -------------------------------------------------------- checks & chores ---

.PHONY: test
test: ## Run the Python unit tests
	$(PY) -m pytest

.PHONY: dbt-run dbt-test dbt-docs
dbt-run: ## Build the dbt models
	$(PY) -m pipelines.cli dbt-run

dbt-test: ## Run the dbt schema and data tests
	$(PY) -m pipelines.cli dbt-test

dbt-docs: ## Generate and serve dbt docs on :8081
	WAREHOUSE_PATH=$(PWD)/data/warehouse/weather.duckdb \
	$(VENV)/bin/dbt docs generate --project-dir dbt/weather --profiles-dir dbt/weather
	WAREHOUSE_PATH=$(PWD)/data/warehouse/weather.duckdb \
	$(VENV)/bin/dbt docs serve --project-dir dbt/weather --profiles-dir dbt/weather --port 8081

.PHONY: clean
clean: ## Delete the warehouse, landed files and dbt/Airflow artefacts
	rm -rf data/warehouse dbt/weather/target dbt/weather/logs airflow_home
	rm -f data/landing/*.json

.env:
	cp .env.example .env

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
