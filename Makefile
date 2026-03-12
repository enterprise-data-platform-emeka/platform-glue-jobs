# Glue Jobs — developer task runner
#
# Run `make help` to see all available commands.
# Run `make setup` once after cloning to install host-side tools.
# Run `make pull` once to pull the AWS Glue Docker image (~3 GB).
#
# Local development loop:
#   1. make setup           — install host-side Python tools
#   2. make pull            — pull the AWS Glue Docker image
#   3. make generate-bronze — create test Parquet files in data/bronze/
#   4. make run-all         — run all 4 Silver jobs inside Docker
#   5. make test            — run the test suite on the host
#
# Deploying to AWS Glue:
#   make deploy ENV=dev     — upload job scripts to S3 and create/update Glue jobs

.DEFAULT_GOAL := help

PYTHON  := .venv/bin/python
PIP     := .venv/bin/pip
PYTEST  := .venv/bin/pytest
RUFF    := .venv/bin/ruff
MYPY    := .venv/bin/mypy

ENV ?= dev

# S3 paths for AWS deployment — read from environment or use dev defaults
BRONZE_BUCKET ?= edp-$(ENV)-$(AWS_ACCOUNT_ID)-bronze
SILVER_BUCKET ?= edp-$(ENV)-$(AWS_ACCOUNT_ID)-silver
QUARANTINE_BUCKET ?= edp-$(ENV)-$(AWS_ACCOUNT_ID)-quarantine
GLUE_SCRIPTS_BUCKET ?= edp-$(ENV)-$(AWS_ACCOUNT_ID)-athena-results

# Local data paths (inside Docker container)
LOCAL_BRONZE    := file:///home/glue_user/workspace/data/bronze
LOCAL_SILVER    := file:///home/glue_user/workspace/data/silver
LOCAL_QUARANTINE := file:///home/glue_user/workspace/data/quarantine

# Docker run shorthand — runs a command inside the Glue container then removes it
GLUE_RUN := docker compose run --rm glue

.PHONY: help setup pull generate-bronze \
        run-dim-customer run-dim-product run-fact-orders run-fact-order-items run-all \
        test lint typecheck deploy clean

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "Glue Jobs — available commands:"
	@echo ""
	@echo "  Setup"
	@echo "    make setup              Install host-side Python tools into .venv"
	@echo "    make pull               Pull the AWS Glue Docker image (~3 GB, one-time)"
	@echo ""
	@echo "  Local development"
	@echo "    make generate-bronze    Generate test Bronze Parquet files in data/bronze/"
	@echo "    make run-dim-customer   Run the dim_customer Silver job locally"
	@echo "    make run-dim-product    Run the dim_product Silver job locally"
	@echo "    make run-fact-orders    Run the fact_orders Silver job locally"
	@echo "    make run-fact-order-items  Run the fact_order_items Silver job locally"
	@echo "    make run-all            Run all 4 Silver jobs locally (in dependency order)"
	@echo ""
	@echo "  Code quality"
	@echo "    make test               Run the test suite"
	@echo "    make lint               Run ruff linter"
	@echo "    make typecheck          Run mypy type checker"
	@echo ""
	@echo "  AWS deployment"
	@echo "    make deploy ENV=dev     Upload scripts to S3 and update Glue job definitions"
	@echo ""
	@echo "  Cleanup"
	@echo "    make clean              Remove .venv and Python cache files"
	@echo ""

# ── Setup ─────────────────────────────────────────────────────────────────────

setup:
	@echo "Creating virtual environment..."
	python -m venv .venv
	$(PIP) install --upgrade pip --quiet
	$(PIP) install -r requirements.txt -r requirements-dev.txt --quiet
	@echo ""
	@echo "Setup complete. Run 'make pull' to pull the AWS Glue Docker image."

pull:
	@echo "Pulling AWS Glue 4.0 Docker image (~3 GB)..."
	docker pull --platform linux/amd64 amazon/aws-glue-libs:glue_libs_4.0.0_image_01
	@echo "Image ready."

# ── Bronze test data ──────────────────────────────────────────────────────────

generate-bronze:
	@echo "Generating test Bronze Parquet files in data/bronze/..."
	@mkdir -p data/bronze data/silver data/quarantine
	$(PYTHON) scripts/generate_bronze.py
	@echo "Bronze data ready in data/bronze/raw/public/"

# ── Local Glue jobs ───────────────────────────────────────────────────────────
#
# Each job runs inside the AWS Glue Docker container via gluesparksubmit.
# gluesparksubmit is a wrapper around spark-submit that configures the full
# Glue classpath (awsglue jars, Hive metastore, etc.) exactly as AWS does.
# The --extra-py-files flag makes lib/ importable inside the Spark workers.

run-dim-customer:
	@echo "Running dim_customer job..."
	$(GLUE_RUN) gluesparksubmit \
		--extra-py-files /home/glue_user/workspace/lib \
		jobs/dim_customer.py \
		--JOB_NAME dim_customer \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-dim-product:
	@echo "Running dim_product job..."
	$(GLUE_RUN) gluesparksubmit \
		--extra-py-files /home/glue_user/workspace/lib \
		jobs/dim_product.py \
		--JOB_NAME dim_product \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-fact-orders:
	@echo "Running fact_orders job..."
	$(GLUE_RUN) gluesparksubmit \
		--extra-py-files /home/glue_user/workspace/lib \
		jobs/fact_orders.py \
		--JOB_NAME fact_orders \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-fact-order-items:
	@echo "Running fact_order_items job..."
	$(GLUE_RUN) gluesparksubmit \
		--extra-py-files /home/glue_user/workspace/lib \
		jobs/fact_order_items.py \
		--JOB_NAME fact_order_items \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-all: run-dim-customer run-dim-product run-fact-orders run-fact-order-items
	@echo ""
	@echo "All Silver jobs complete. Output in data/silver/"

# ── Code quality ──────────────────────────────────────────────────────────────

test:
	@echo "Running tests..."
	$(PYTEST) tests/ --cov=scripts --cov-report=term-missing

lint:
	@echo "Running ruff..."
	$(RUFF) check scripts/ tests/

typecheck:
	@echo "Running mypy..."
	$(MYPY) scripts/ --ignore-missing-imports

# ── AWS deployment ────────────────────────────────────────────────────────────
#
# Uploads job scripts to the Glue scripts bucket, then creates or updates
# each Glue job via the AWS CLI. The jobs read from s3:// paths instead of
# file:// — no code changes needed, only the --BRONZE_PATH parameter changes.

deploy:
	@echo "Deploying Glue jobs to AWS ($(ENV))..."
	@if [ -z "$(AWS_ACCOUNT_ID)" ]; then \
		echo "ERROR: AWS_ACCOUNT_ID is not set. Export it first:"; \
		echo "  export AWS_ACCOUNT_ID=\$$(aws sts get-caller-identity --query Account --output text --profile $(ENV)-admin)"; \
		exit 1; \
	fi
	@echo "Uploading job scripts to s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/..."
	aws s3 sync jobs/ s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/ \
		--exclude "*" --include "*.py" --profile $(ENV)-admin
	aws s3 sync lib/ s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/lib/ \
		--exclude "*" --include "*.py" --profile $(ENV)-admin
	@echo "Creating/updating Glue job definitions..."
	@for job in dim_customer dim_product fact_orders fact_order_items; do \
		echo "  Updating $$job..."; \
		aws glue create-job \
			--name edp-$(ENV)-$$job \
			--role edp-$(ENV)-glue-role \
			--command "Name=glueetl,ScriptLocation=s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/$$job.py,PythonVersion=3" \
			--default-arguments "{ \
				\"--job-language\": \"python\", \
				\"--extra-py-files\": \"s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/lib/\", \
				\"--BRONZE_PATH\": \"s3://$(BRONZE_BUCKET)/\", \
				\"--SILVER_PATH\": \"s3://$(SILVER_BUCKET)/\", \
				\"--QUARANTINE_PATH\": \"s3://$(QUARANTINE_BUCKET)/\", \
				\"--enable-glue-datacatalog\": \"true\", \
				\"--enable-job-insights\": \"true\" \
			}" \
			--glue-version "4.0" \
			--number-of-workers 2 \
			--worker-type G.1X \
			--profile $(ENV)-admin 2>/dev/null \
			|| aws glue update-job \
				--job-name edp-$(ENV)-$$job \
				--job-update "Command={Name=glueetl,ScriptLocation=s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/$$job.py,PythonVersion=3}" \
				--profile $(ENV)-admin; \
	done
	@echo "Deployment complete."

# ── Cleanup ───────────────────────────────────────────────────────────────────

clean:
	rm -rf .venv
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned."
