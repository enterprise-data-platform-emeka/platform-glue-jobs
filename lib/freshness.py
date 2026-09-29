"""Publish DMS ingestion age against UTC now, separately from business history.

Backdated seed orders are expected. Their DMS transport timestamps are current;
coverage of business dates is checked against the dataset contract by dbt.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

_NAMESPACE = "EDP/DataFreshness"
_METRIC_NAME = "SilverDataAgeHours"


def publish_freshness_metric(
    table_name: str,
    max_dms_timestamp: str | None,
    job_name: str,
) -> None:
    """
    Compute data age relative to datetime.now(timezone.utc) and publish to CloudWatch.

    Args:
        table_name:        Silver table name (e.g. "dim_customer").
        max_dms_timestamp: String value of max(_dms_timestamp) from Bronze,
                           format "YYYY-MM-DD HH:MM:SS[.ffffff]". None if the
                           Bronze table was empty.
        job_name:          Glue JOB_NAME arg (e.g. "edp-dev-dim_customer").
                           Environment is derived from the second segment.
    """
    if max_dms_timestamp is None:
        print(
            f"[freshness] {table_name}: max(_dms_timestamp) is None — Bronze table empty, skipping metric.",
            file=sys.stderr,
        )
        return

    try:
        import boto3

        ts = datetime.fromisoformat(max_dms_timestamp)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
        environment = job_name.split("-")[1] if "-" in job_name else "unknown"

        boto3.client("cloudwatch").put_metric_data(
            Namespace=_NAMESPACE,
            MetricData=[
                {
                    "MetricName": _METRIC_NAME,
                    "Value": age_hours,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "Table", "Value": table_name},
                        {"Name": "Environment", "Value": environment},
                    ],
                }
            ],
        )
        print(
            f"[freshness] {table_name}: max(_dms_timestamp)={max_dms_timestamp}, "
            f"age_hours={age_hours:.2f} (reference={datetime.now(timezone.utc).isoformat()})"
        )
    except Exception as exc:
        print(
            f"[freshness] {table_name}: CloudWatch publish skipped (local mode or no credentials): {exc}",
            file=sys.stderr,
        )


_ROW_COUNT_NAMESPACE = "EDP/DataQuality"
_ROW_COUNT_METRIC = "SilverRowCount"


def publish_row_count_metric(
    table_name: str,
    row_count: int,
    job_name: str,
) -> None:
    """
    Publish Silver output row count to CloudWatch after each Glue job write.

    The DAG validation task (validate_silver_row_counts) reads this metric to
    confirm every Silver table received rows in the current pipeline run. Using
    CloudWatch avoids a separate Athena COUNT(*) scan at validation time.

    Namespace:  EDP/DataQuality
    MetricName: SilverRowCount
    Unit:       Count
    Dimensions: Table={table_name}, Environment={dev|staging|prod}
    Value:      Number of rows written to Silver in this job run.
    """
    try:
        import boto3

        environment = job_name.split("-")[1] if "-" in job_name else "unknown"

        boto3.client("cloudwatch").put_metric_data(
            Namespace=_ROW_COUNT_NAMESPACE,
            MetricData=[
                {
                    "MetricName": _ROW_COUNT_METRIC,
                    "Value": float(row_count),
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "Table", "Value": table_name},
                        {"Name": "Environment", "Value": environment},
                    ],
                }
            ],
        )
        print(f"[row-count] {table_name}: {row_count:,} rows published to CloudWatch")
    except Exception as exc:
        print(
            f"[row-count] {table_name}: CloudWatch publish skipped (local mode or no credentials): {exc}",
            file=sys.stderr,
        )
