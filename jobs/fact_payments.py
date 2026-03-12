"""
Silver job: fact_payments

Reads the Bronze payments table, reconciles CDC operations to get the current
state of every payment, validates data quality, and writes the clean fact table
to Silver partitioned by payment date.

One payment exists per order. Payments are the financial record of the platform
— the amount column is what we aggregate when calculating revenue.

Input (Bronze):
    {BRONZE_PATH}/raw/public/payments/
    Columns: Op, _dms_timestamp, payment_id, order_id, method, amount,
             status, payment_date, updated_at

Output (Silver):
    {SILVER_PATH}/fact_payments/
    Columns: payment_id, order_id, method, amount, status, payment_date,
             payment_year, payment_month
    Partitioned by: payment_year, payment_month

Quarantine:
    {QUARANTINE_PATH}/fact_payments/
    Rows that failed validation, plus a _validation_errors column.

Validation rules:
    - payment_id must not be null
    - order_id must not be null
    - method must not be null
    - amount must be greater than zero
    - status must not be null
    - payment_date must not be null
    - payment_year must not be null (guards against null payment_date)
    - payment_month must not be null

Business questions this fact table enables (in Gold via dbt/Athena):
    - Total revenue by month (SUM(amount) WHERE status = 'completed')
    - Revenue breakdown by payment method (GROUP BY method)
    - Failed payment rate (COUNT WHERE status = 'failed' / COUNT total)
    - Refund volume over time (SUM(amount) WHERE status = 'refunded')
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from lib.cdc import reconcile
from lib.job_utils import commit_job, init_job
from lib.paths import resolve_paths
from lib.schemas import PAYMENTS_SCHEMA
from lib.validation import validate

# ── Job initialisation ────────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "BRONZE_PATH", "SILVER_PATH", "QUARANTINE_PATH"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
init_job(job, args["JOB_NAME"], args)

paths = resolve_paths(args)

# ── Read Bronze ───────────────────────────────────────────────────────────────

print(f"[fact_payments] Reading Bronze from {paths.bronze_table('payments')}")
bronze_df = spark.read.schema(PAYMENTS_SCHEMA).parquet(paths.bronze_table("payments"))

# ── CDC reconciliation ────────────────────────────────────────────────────────
#
# A payment can be updated (e.g. status changes from 'pending' to 'completed'
# or 'failed'). CDC reconciliation keeps only the latest version.

current_df = reconcile(bronze_df, pk_col="payment_id")

# ── Build fact table columns ──────────────────────────────────────────────────
#
# Derive payment_year and payment_month for partitioning. This aligns with
# fact_orders and fact_order_items so dbt/Athena can join across them using
# the same partition filter — a query for "January 2024 revenue" scans the
# same partition in all three fact tables.

fact_df = current_df.select(
    "payment_id",
    "order_id",
    "method",
    "amount",
    "status",
    "payment_date",
    F.year("payment_date").alias("payment_year"),
    F.month("payment_date").alias("payment_month"),
)

# ── Validate ──────────────────────────────────────────────────────────────────

RULES = {
    "payment_id_not_null": "payment_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
    "method_not_null": "method IS NOT NULL",
    "amount_positive": "amount > 0",
    "status_not_null": "status IS NOT NULL",
    "payment_date_not_null": "payment_date IS NOT NULL",
    # Guard against null partition values from null payment_date.
    "payment_year_not_null": "payment_year IS NOT NULL",
    "payment_month_not_null": "payment_month IS NOT NULL",
}

clean_df = validate(fact_df, RULES, paths.quarantine_root, "fact_payments")

# ── Write Silver ──────────────────────────────────────────────────────────────

silver_path = paths.silver_table("fact_payments")
print(f"[fact_payments] Writing Silver to {silver_path}")
clean_df.write.mode("overwrite").partitionBy("payment_year", "payment_month").parquet(silver_path)
print("[fact_payments] Done.")

commit_job(job)
