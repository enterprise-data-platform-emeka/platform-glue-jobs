"""
run_dbt.py: Glue Python Shell job that runs dbt against Athena to produce Gold tables.

This job is triggered by the Step Functions pipeline after the Silver crawler
completes. It replaces the BashOperator dbt task from the MWAA DAG.

What it does:
  1. Downloads the dbt project from S3 (bronze bucket, dbt/ prefix) to /tmp/dbt_workspace.
     The platform-session-orchestrator syncs the project there before triggering Step Functions.
  2. D2: dbt test --select source:silver — verifies Silver is not stale before running models.
  3. dbt deps -> dbt run — builds Gold tables.
  4. D3: Gold vs Silver row count validation — compares staging model row counts (from
     run_results.json) against Silver CloudWatch metrics. Fails if any staging model
     diverges from its Silver source by more than 5%.
  5. dbt test — runs Gold model quality assertions.
  6. Uploads manifest.json and catalog.json to s3://{bronze}/metadata/dbt/ so the
     Analytics Agent can load business context at startup.

Job parameters (passed via --default-arguments or Step Functions Arguments override):
  --DBT_TARGET             dev | staging | prod
  --BRONZE_BUCKET          S3 bucket name (no s3:// prefix)
  --ATHENA_RESULTS_BUCKET  S3 bucket name for Athena query results
  --ATHENA_WORKGROUP       Athena workgroup name
  --DBT_ATHENA_SCHEMA      Glue database name for Gold tables (e.g. edp_dev_gold)
  --DBT_SILVER_SCHEMA      Glue database name for Silver tables (e.g. edp_dev_silver)
  --AWS_DEFAULT_REGION     AWS region (e.g. eu-central-1)

Installed at job startup via --additional-python-modules:
  dbt-core==1.8.7
  dbt-athena-community==1.8.3
"""

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, [
        "DBT_TARGET",
        "BRONZE_BUCKET",
        "ATHENA_RESULTS_BUCKET",
        "ATHENA_WORKGROUP",
        "DBT_ATHENA_SCHEMA",
        "DBT_SILVER_SCHEMA",
        "AWS_DEFAULT_REGION",
    ])
except ImportError:
    # Local dev fallback: read from environment variables directly.
    args = {
        "DBT_TARGET":            os.environ["DBT_TARGET"],
        "BRONZE_BUCKET":         os.environ["BRONZE_BUCKET"],
        "ATHENA_RESULTS_BUCKET": os.environ["ATHENA_RESULTS_BUCKET"],
        "ATHENA_WORKGROUP":      os.environ["ATHENA_WORKGROUP"],
        "DBT_ATHENA_SCHEMA":     os.environ["DBT_ATHENA_SCHEMA"],
        "DBT_SILVER_SCHEMA":     os.environ.get("DBT_SILVER_SCHEMA", "edp_dev_silver"),
        "AWS_DEFAULT_REGION":    os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),
    }

DBT_TARGET            = args["DBT_TARGET"]
BRONZE_BUCKET         = args["BRONZE_BUCKET"]
ATHENA_RESULTS_BUCKET = args["ATHENA_RESULTS_BUCKET"]
ATHENA_WORKGROUP      = args["ATHENA_WORKGROUP"]
DBT_ATHENA_SCHEMA     = args["DBT_ATHENA_SCHEMA"]
DBT_SILVER_SCHEMA     = args["DBT_SILVER_SCHEMA"]
AWS_DEFAULT_REGION    = args["AWS_DEFAULT_REGION"]

_SILVER_TO_STAGING = {
    "dim_customer":    "stg_customers",
    "dim_product":     "stg_products",
    "fact_orders":     "stg_orders",
    "fact_order_items": "stg_order_items",
    "fact_payments":   "stg_payments",
    "fact_shipments":  "stg_shipments",
}

DBT_S3_PREFIX    = "dbt/platform-dbt-analytics/"
DBT_WORKSPACE    = "/tmp/dbt_workspace"
DBT_PROFILES_DIR = f"{DBT_WORKSPACE}/profiles"
ARTIFACT_S3_PREFIX = "metadata/dbt/"


def download_dbt_project() -> None:
    """Download the dbt project from S3 to /tmp/dbt_workspace using boto3.

    Uses boto3 directly instead of shelling out to the AWS CLI. This is
    more reliable inside Glue Python Shell because it uses the job's IAM
    role natively and surfaces real error messages on failure.
    """
    print(f"Downloading dbt project from s3://{BRONZE_BUCKET}/{DBT_S3_PREFIX}")

    if os.path.exists(DBT_WORKSPACE):
        shutil.rmtree(DBT_WORKSPACE)
    os.makedirs(DBT_WORKSPACE)

    s3 = boto3.client("s3", region_name=AWS_DEFAULT_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    count = 0

    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=DBT_S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            relative_path = key[len(DBT_S3_PREFIX):]
            if not relative_path:
                continue
            local_path = os.path.join(DBT_WORKSPACE, relative_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(BRONZE_BUCKET, key, local_path)
            count += 1

    if count == 0:
        raise RuntimeError(
            f"No files found at s3://{BRONZE_BUCKET}/{DBT_S3_PREFIX}. "
            "Run the sync-dbt step in the session-start workflow before triggering the pipeline."
        )

    print(f"Downloaded {count} files to {DBT_WORKSPACE}")


def run_dbt_command(command: list[str]) -> None:
    """Run a dbt command as a subprocess.

    Uses the dbt CLI binary installed alongside sys.executable by
    --additional-python-modules. dbt has no __main__ so python -m dbt
    does not work in Glue Python Shell.
    """
    dbt_bin = os.path.join(os.path.dirname(sys.executable), "dbt")
    full_command = [
        dbt_bin,
        *command,
        "--target", DBT_TARGET,
        "--profiles-dir", DBT_PROFILES_DIR,
        "--no-use-colors",
    ]
    print(f"Running: {' '.join(full_command)}")

    result = subprocess.run(
        full_command,
        cwd=DBT_WORKSPACE,
        env={
            **os.environ,
            "DBT_TARGET":            DBT_TARGET,
            "ATHENA_RESULTS_BUCKET": ATHENA_RESULTS_BUCKET,
            "ATHENA_WORKGROUP":      ATHENA_WORKGROUP,
            "DBT_ATHENA_SCHEMA":     DBT_ATHENA_SCHEMA,
            "DBT_SILVER_SCHEMA":     DBT_SILVER_SCHEMA,
            "AWS_DEFAULT_REGION":    AWS_DEFAULT_REGION,
        },
        check=False,
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"dbt command failed with exit code {result.returncode}: {' '.join(command)}"
        )


def upload_dbt_artifacts() -> None:
    """Upload dbt manifest.json and catalog.json to S3.

    The Analytics Agent reads these at startup to enrich Gold table schemas
    with business context from dbt model descriptions and column docs.
    """
    s3 = boto3.client("s3", region_name=AWS_DEFAULT_REGION)
    for artifact in ("manifest.json", "catalog.json"):
        local_path = os.path.join(DBT_WORKSPACE, "target", artifact)
        s3_key = f"{ARTIFACT_S3_PREFIX}{artifact}"

        if not os.path.exists(local_path):
            print(f"WARNING: {artifact} not found at {local_path}, skipping upload.")
            continue

        try:
            s3.upload_file(local_path, BRONZE_BUCKET, s3_key)
            print(f"Uploaded {artifact} to s3://{BRONZE_BUCKET}/{s3_key}")
        except ClientError as exc:
            print(f"WARNING: Failed to upload {artifact}: {exc}", file=sys.stderr)


def validate_gold_row_counts() -> None:
    """
    D3: Compare Gold staging model row counts against Silver CloudWatch row count metrics.

    Reads staging model row counts from dbt's run_results.json (written by dbt run).
    Reads Silver row counts from the EDP/DataQuality CloudWatch namespace published by
    the Glue Silver jobs. Fails if any staging model diverges from its Silver source
    by more than 5%.

    Uses CloudWatch metrics and the dbt artifact file rather than Athena COUNT(*) scans.
    Falls back gracefully if dbt-athena reports rows_affected=-1 (some adapter versions
    do not populate this for CTAS queries) — logs a warning but does not fail the job.
    """
    # ── Silver counts from CloudWatch ─────────────────────────────────────────
    cw = boto3.client("cloudwatch", region_name=AWS_DEFAULT_REGION)
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=2)

    silver_counts: dict[str, int | None] = {}
    for table in _SILVER_TO_STAGING:
        resp = cw.get_metric_statistics(
            Namespace="EDP/DataQuality",
            MetricName="SilverRowCount",
            Dimensions=[
                {"Name": "Table", "Value": table},
                {"Name": "Environment", "Value": DBT_TARGET},
            ],
            StartTime=start,
            EndTime=now,
            Period=7200,
            Statistics=["Maximum"],
        )
        dps = resp.get("Datapoints", [])
        silver_counts[table] = int(dps[0]["Maximum"]) if dps else None

    # ── Gold staging counts from run_results.json ─────────────────────────────
    results_path = os.path.join(DBT_WORKSPACE, "target", "run_results.json")
    if not os.path.exists(results_path):
        print("[gold-row-count] run_results.json not found — skipping Gold vs Silver comparison")
        return

    with open(results_path) as f:
        run_results = json.load(f)

    stg_counts: dict[str, int] = {}
    for result in run_results.get("results", []):
        uid = result.get("unique_id", "")
        if not uid.startswith("model.") or "stg_" not in uid:
            continue
        model_name = uid.split(".")[-1]
        rows = result.get("adapter_response", {}).get("rows_affected", -1)
        if rows >= 0:
            stg_counts[model_name] = rows

    if not stg_counts:
        print(
            "[gold-row-count] rows_affected=-1 for all staging models "
            "(adapter does not populate this for CTAS). Skipping comparison."
        )
        return

    # ── Compare ───────────────────────────────────────────────────────────────
    mismatches = []
    for silver_table, stg_model in _SILVER_TO_STAGING.items():
        silver_count = silver_counts.get(silver_table)
        gold_count = stg_counts.get(stg_model)

        if silver_count is None or gold_count is None:
            print(
                f"[gold-row-count] {stg_model}: missing count "
                f"(silver={silver_count}, gold={gold_count}) — skipping"
            )
            continue

        print(f"[gold-row-count] {stg_model}: silver={silver_count:,}, gold={gold_count:,}")

        if silver_count > 0:
            divergence = abs(silver_count - gold_count) / silver_count
            if divergence > 0.05:
                mismatches.append(
                    f"{stg_model}: silver={silver_count:,}, gold={gold_count:,}, "
                    f"divergence={divergence:.1%} (threshold 5%)"
                )

    if mismatches:
        raise RuntimeError(
            f"Gold vs Silver row count divergence exceeded: {'; '.join(mismatches)}"
        )


def main() -> None:
    print(f"=== EDP dbt Gold run starting (target: {DBT_TARGET}) ===")

    download_dbt_project()

    print("--- dbt deps ---")
    run_dbt_command(["deps"])

    print("--- D2: dbt source freshness gate ---")
    run_dbt_command(["test", "--select", "source:silver"])

    print("--- dbt run ---")
    run_dbt_command(["run"])

    print("--- D3: Gold vs Silver row count validation ---")
    validate_gold_row_counts()

    print("--- dbt test ---")
    run_dbt_command(["test"])

    upload_dbt_artifacts()

    print("=== EDP dbt Gold run complete ===")


if __name__ == "__main__":
    main()
