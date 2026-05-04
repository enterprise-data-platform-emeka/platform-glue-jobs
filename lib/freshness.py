"""
Data freshness metric publisher for Silver Glue jobs.

Each Silver job captures max(_dms_timestamp) from Bronze before CDC reconciliation
(reconcile() drops _dms_timestamp from the output). That timestamp is then compared
against a fixed reference date — the latest date present in the dataset — to compute
how stale the data is relative to the expected cutoff.

Why a reference date instead of wall-clock time:
    The Bronze dataset is a historical snapshot ending on 2026-03-02. Measuring
    freshness against datetime.now() would always show ~56+ days of staleness.
    Measuring against the reference date instead answers the useful question:
    "Does Silver have data up to the expected cutoff?" If max(_dms_timestamp)
    is within 24 hours of 2026-03-02 23:59:59, the pipeline processed all
    available Bronze data correctly.

Metric published:
    Namespace:  EDP/DataFreshness
    MetricName: SilverDataAgeHours
    Unit:       Count (CloudWatch has no Hours unit)
    Dimensions: Table={table_name}, Environment={dev|staging|prod}
    Value:      Hours between max(_dms_timestamp) and REFERENCE_DATE.
                Positive = data is older than reference (potentially stale).
                Negative or zero = data is at or newer than reference (fresh).

CloudWatch alarm threshold: fire when SilverDataAgeHours > 24.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

REFERENCE_DATE = datetime(2026, 3, 2, 23, 59, 59, tzinfo=timezone.utc)
_NAMESPACE = "EDP/DataFreshness"
_METRIC_NAME = "SilverDataAgeHours"


def publish_freshness_metric(
    table_name: str,
    max_dms_timestamp: str | None,
    job_name: str,
) -> None:
    """
    Compute data age relative to REFERENCE_DATE and publish to CloudWatch.

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

        age_hours = (REFERENCE_DATE - ts).total_seconds() / 3600
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
            f"age_hours={age_hours:.2f} (reference={REFERENCE_DATE.isoformat()})"
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
