"""
run_dbt.py: Glue Python Shell job that runs dbt against Athena to produce Gold tables.

This job is triggered by the Step Functions pipeline after the Silver crawler
completes. It replaces the BashOperator dbt task from the MWAA DAG.

What it does:
  1. Downloads the dbt project from S3 (bronze bucket, dbt/ prefix) to /tmp/dbt_workspace.
     The platform-session-orchestrator syncs the project there before triggering Step Functions.
  2. Runs: dbt deps -> dbt run -> dbt test
  3. Uploads manifest.json and catalog.json to s3://{bronze}/metadata/dbt/ so the
     Analytics Agent can load business context at startup.

Job parameters (passed via --default-arguments or Step Functions Arguments override):
  --DBT_TARGET             dev | staging | prod
  --BRONZE_BUCKET          S3 bucket name (no s3:// prefix)
  --ATHENA_RESULTS_BUCKET  S3 bucket name for Athena query results
  --ATHENA_WORKGROUP       Athena workgroup name
  --DBT_ATHENA_SCHEMA      Glue database name for Gold tables (e.g. edp_dev_gold)
  --AWS_DEFAULT_REGION     AWS region (e.g. eu-central-1)

Installed at job startup via --additional-python-modules:
  dbt-core==1.8.7
  dbt-athena-community==1.8.3
"""

import os
import subprocess
import sys
import shutil
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
        "AWS_DEFAULT_REGION":    os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),
    }

DBT_TARGET            = args["DBT_TARGET"]
BRONZE_BUCKET         = args["BRONZE_BUCKET"]
ATHENA_RESULTS_BUCKET = args["ATHENA_RESULTS_BUCKET"]
ATHENA_WORKGROUP      = args["ATHENA_WORKGROUP"]
DBT_ATHENA_SCHEMA     = args["DBT_ATHENA_SCHEMA"]
AWS_DEFAULT_REGION    = args["AWS_DEFAULT_REGION"]

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

    Uses `python -m dbt` to ensure the dbt installed by
    --additional-python-modules is used, regardless of PATH.
    Streams stdout/stderr directly so CloudWatch captures all output.
    """
    full_command = [
        sys.executable, "-m", "dbt",
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


def main() -> None:
    print(f"=== EDP dbt Gold run starting (target: {DBT_TARGET}) ===")

    download_dbt_project()

    print("--- dbt deps ---")
    run_dbt_command(["deps"])

    print("--- dbt run ---")
    run_dbt_command(["run"])

    print("--- dbt test ---")
    run_dbt_command(["test"])

    upload_dbt_artifacts()

    print("=== EDP dbt Gold run complete ===")


if __name__ == "__main__":
    main()
