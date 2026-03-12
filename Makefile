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
#   4. make run-all         — run all 6 Silver jobs inside Docker
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
GLUE_SCRIPTS_BUCKET ?= edp-$(ENV)-$(AWS_ACCOUNT_ID)-glue-scripts

# Local data paths (inside Docker container)
LOCAL_BRONZE    := file:///home/glue_user/workspace/data/bronze
LOCAL_SILVER    := file:///home/glue_user/workspace/data/silver
LOCAL_QUARANTINE := file:///home/glue_user/workspace/data/quarantine

# Docker run shorthand — runs a command inside the Glue container then removes it.
# GLUE_SUBMIT is the full path to gluesparksubmit inside the container. The image's
# entrypoint sets up SSL and the Spark History Server before exec'ing this binary.
# Using the full path avoids relying on PATH which is not set when the entrypoint
# hands off to user code.
GLUE_RUN    := docker compose run --rm glue
# spark-submit is the correct entrypoint in glue_libs_4.0.0_image_01.
# gluesparksubmit does not exist in this image — it was removed in favour of
# plain spark-submit with the Glue JARs pre-configured in spark-defaults.conf.
# The image entrypoint sources .bashrc, which adds PyGlue.zip and spark/bin
# to PYTHONPATH and PATH respectively, so awsglue imports and lib/ imports
# both work without --extra-py-files.
GLUE_SUBMIT := /home/glue_user/spark/bin/spark-submit

.PHONY: help setup pull generate-bronze \
        run-dim-customer run-dim-product \
        run-fact-orders run-fact-order-items run-fact-payments run-fact-shipments \
        run-all \
        spark-ui \
        trigger-all status-all \
        test lint typecheck deploy package-lib clean

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
	@echo "    make spark-ui           Start Spark History Server at http://localhost:18080"
	@echo "    make run-dim-customer      Run the dim_customer Silver job locally"
	@echo "    make run-dim-product       Run the dim_product Silver job locally"
	@echo "    make run-fact-orders       Run the fact_orders Silver job locally"
	@echo "    make run-fact-order-items  Run the fact_order_items Silver job locally"
	@echo "    make run-fact-payments     Run the fact_payments Silver job locally"
	@echo "    make run-fact-shipments    Run the fact_shipments Silver job locally"
	@echo "    make run-all               Run all 6 Silver jobs locally (in dependency order)"
	@echo ""
	@echo "  Code quality"
	@echo "    make test               Run the test suite"
	@echo "    make lint               Run ruff linter"
	@echo "    make typecheck          Run mypy type checker"
	@echo ""
	@echo "  AWS deployment"
	@echo "    make deploy ENV=dev     Upload scripts to S3 and update Glue job definitions"
	@echo "    make trigger-all ENV=dev  Start all 6 Silver jobs in AWS Glue"
	@echo "    make status-all ENV=dev   Check the status of the last run for each job"
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
# Each job runs inside the AWS Glue Docker container via spark-submit.
# spark-defaults.conf (pre-configured in the image) adds all Glue JARs to the
# driver and executor classpaths, so awsglue Java classes are available.
# The image entrypoint sources .bashrc which puts PyGlue.zip (awsglue Python)
# and our workspace directory on PYTHONPATH, so lib/ imports work in local mode
# without --extra-py-files (local[*] shares the driver's PYTHONPATH with workers).

run-dim-customer:
	@echo "Running dim_customer job..."
	$(GLUE_RUN) $(GLUE_SUBMIT) \
		jobs/dim_customer.py \
		--JOB_NAME dim_customer \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-dim-product:
	@echo "Running dim_product job..."
	$(GLUE_RUN) $(GLUE_SUBMIT) \
		jobs/dim_product.py \
		--JOB_NAME dim_product \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-fact-orders:
	@echo "Running fact_orders job..."
	$(GLUE_RUN) $(GLUE_SUBMIT) \
		jobs/fact_orders.py \
		--JOB_NAME fact_orders \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-fact-order-items:
	@echo "Running fact_order_items job..."
	$(GLUE_RUN) $(GLUE_SUBMIT) \
		jobs/fact_order_items.py \
		--JOB_NAME fact_order_items \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-fact-payments:
	@echo "Running fact_payments job..."
	$(GLUE_RUN) $(GLUE_SUBMIT) \
		jobs/fact_payments.py \
		--JOB_NAME fact_payments \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-fact-shipments:
	@echo "Running fact_shipments job..."
	$(GLUE_RUN) $(GLUE_SUBMIT) \
		jobs/fact_shipments.py \
		--JOB_NAME fact_shipments \
		--BRONZE_PATH $(LOCAL_BRONZE) \
		--SILVER_PATH $(LOCAL_SILVER) \
		--QUARANTINE_PATH $(LOCAL_QUARANTINE)

run-all: run-dim-customer run-dim-product run-fact-orders run-fact-order-items run-fact-payments run-fact-shipments
	@echo ""
	@echo "All Silver jobs complete. Output in data/silver/"

spark-ui:
	@echo "Starting Spark History Server at http://localhost:18080 ..."
	@mkdir -p spark-events
	docker compose up history-server

# ── AWS job triggers ──────────────────────────────────────────────────────────

trigger-all:
	@echo "Triggering all Silver jobs in AWS ($(ENV))..."
	@if [ -z "$(AWS_ACCOUNT_ID)" ]; then \
		echo "ERROR: AWS_ACCOUNT_ID is not set. Export it first:"; \
		echo "  export AWS_ACCOUNT_ID=\$$(aws sts get-caller-identity --query Account --output text --profile $(ENV)-admin)"; \
		exit 1; \
	fi
	@for job in dim_customer dim_product fact_orders fact_order_items fact_payments fact_shipments; do \
		echo -n "  Starting edp-$(ENV)-$$job ... "; \
		aws glue start-job-run --job-name edp-$(ENV)-$$job \
			--profile $(ENV)-admin \
			--query 'JobRunId' --output text; \
	done
	@echo ""
	@echo "All jobs triggered. Run 'make status-all ENV=$(ENV)' to check progress."

status-all:
	@echo "Job status in AWS ($(ENV)):"
	@echo ""
	@for job in dim_customer dim_product fact_orders fact_order_items fact_payments fact_shipments; do \
		printf "  %-25s " "edp-$(ENV)-$$job"; \
		aws glue get-job-runs --job-name edp-$(ENV)-$$job \
			--profile $(ENV)-admin \
			--max-results 1 \
			--query 'JobRuns[0].{State:JobRunState,Duration:ExecutionTime,Error:ErrorMessage}' \
			--output text 2>/dev/null || echo "NO RUNS YET"; \
	done

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
# Uploads job scripts and a lib.zip to the Glue scripts bucket, then creates
# or updates each Glue job via the AWS CLI.
#
# lib/ is zipped because --extra-py-files requires .py or .zip paths, not
# directory paths. The zip preserves the lib/ package structure so that
# "from lib.cdc import reconcile" resolves correctly inside Glue workers.
#
# The jobs read from s3:// paths instead of file:// — no code changes needed,
# only the --BRONZE_PATH/SILVER_PATH/QUARANTINE_PATH parameters change.

package-lib:
	@echo "Packaging lib/ into lib.zip..."
	zip -r lib.zip lib/ -x "lib/__pycache__/*" -x "lib/*.pyc"
	@echo "lib.zip ready."

deploy: package-lib
	@echo "Deploying Glue jobs to AWS ($(ENV))..."
	@if [ -z "$(AWS_ACCOUNT_ID)" ]; then \
		echo "ERROR: AWS_ACCOUNT_ID is not set. Export it first:"; \
		echo "  export AWS_ACCOUNT_ID=\$$(aws sts get-caller-identity --query Account --output text --profile $(ENV)-admin)"; \
		exit 1; \
	fi
	@echo "Uploading job scripts to s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/..."
	aws s3 sync jobs/ s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/ \
		--exclude "*" --include "*.py" --profile $(ENV)-admin
	@echo "Uploading lib.zip to s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/lib.zip..."
	aws s3 cp lib.zip s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/lib.zip \
		--profile $(ENV)-admin
	@echo "Creating/updating Glue job definitions..."
	@for job in dim_customer dim_product fact_orders fact_order_items fact_payments fact_shipments; do \
		echo "  Updating $$job..."; \
		aws glue create-job \
			--name edp-$(ENV)-$$job \
			--role edp-$(ENV)-glue-role \
			--command "Name=glueetl,ScriptLocation=s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/$$job.py,PythonVersion=3" \
			--default-arguments "{ \
				\"--job-language\": \"python\", \
				\"--extra-py-files\": \"s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/lib.zip\", \
				\"--BRONZE_PATH\": \"s3://$(BRONZE_BUCKET)/\", \
				\"--SILVER_PATH\": \"s3://$(SILVER_BUCKET)/\", \
				\"--QUARANTINE_PATH\": \"s3://$(QUARANTINE_BUCKET)/\", \
				\"--enable-glue-datacatalog\": \"true\", \
				\"--enable-job-insights\": \"true\", \
				\"--enable-spark-ui\": \"true\", \
				\"--spark-event-logs-path\": \"s3://$(GLUE_SCRIPTS_BUCKET)/spark-logs/\" \
			}" \
			--glue-version "4.0" \
			--number-of-workers 2 \
			--worker-type G.1X \
			--profile $(ENV)-admin 2>/dev/null \
			|| aws glue update-job \
				--job-name edp-$(ENV)-$$job \
				--job-update "{ \
					\"Role\": \"edp-$(ENV)-glue-role\", \
					\"GlueVersion\": \"4.0\", \
					\"NumberOfWorkers\": 2, \
					\"WorkerType\": \"G.1X\", \
					\"Command\": {\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/$$job.py\", \"PythonVersion\": \"3\"}, \
					\"DefaultArguments\": { \
						\"--job-language\": \"python\", \
						\"--extra-py-files\": \"s3://$(GLUE_SCRIPTS_BUCKET)/glue-scripts/lib.zip\", \
						\"--BRONZE_PATH\": \"s3://$(BRONZE_BUCKET)/\", \
						\"--SILVER_PATH\": \"s3://$(SILVER_BUCKET)/\", \
						\"--QUARANTINE_PATH\": \"s3://$(QUARANTINE_BUCKET)/\", \
						\"--enable-glue-datacatalog\": \"true\", \
						\"--enable-job-insights\": \"true\", \
						\"--enable-spark-ui\": \"true\", \
						\"--spark-event-logs-path\": \"s3://$(GLUE_SCRIPTS_BUCKET)/spark-logs/\" \
					} \
				}" \
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
