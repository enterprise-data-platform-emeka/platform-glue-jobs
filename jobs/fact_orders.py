"""
Silver job: fact_orders

Reads the Bronze orders table, reconciles CDC operations to get the current
state of every order, validates data quality, and writes the clean fact table
to Silver partitioned by order date.

Input (Bronze):
    {BRONZE_PATH}/raw/public/orders/
    Columns: Op, _dms_timestamp, order_id, customer_id, order_date,
             order_status, updated_at

Output (Silver):
    {SILVER_PATH}/fact_orders/
    Columns: order_id, customer_id, order_date, order_status, order_year, order_month
    Partitioned by: order_year, order_month

Quarantine:
    {QUARANTINE_PATH}/fact_orders/
    Rows that failed validation, plus a _validation_errors column.

Validation rules:
    - order_id must not be null
    - customer_id must not be null
    - order_date must not be null
    - order_status must not be null

Why partition by order_year and order_month:
    Partitioning lets dbt and Athena scan only the months they need instead
    of reading the entire table. A query for "revenue this month" only touches
    one partition. Without partitioning, every query scans all orders ever.

Why order_year and order_month as separate integer columns (not strings):
    Integer columns sort and filter correctly. "2024" < "2025" works as
    expected. String partitions like "2024-01" also work but integer columns
    are conventional in Hive-style partitioning and play better with Athena's
    partition projection feature.
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
from lib.schemas import ORDERS_SCHEMA
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

print(f"[fact_orders] Reading Bronze from {paths.bronze_table('orders')}")
bronze_df = spark.read.schema(ORDERS_SCHEMA).parquet(paths.bronze_table("orders"))

# ── CDC reconciliation ────────────────────────────────────────────────────────

current_df = reconcile(bronze_df, pk_col="order_id")

# ── Build fact table columns ──────────────────────────────────────────────────
#
# Add order_year and order_month as integer columns derived from order_date.
# These become the Parquet partition columns so Athena can do partition pruning.

fact_df = current_df.select(
    "order_id",
    "customer_id",
    "order_date",
    "order_status",
    F.year("order_date").alias("order_year"),
    F.month("order_date").alias("order_month"),
)

# ── Validate ──────────────────────────────────────────────────────────────────

RULES = {
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "order_date_not_null": "order_date IS NOT NULL",
    "order_status_not_null": "order_status IS NOT NULL",
}

clean_df = validate(fact_df, RULES, paths.quarantine_root, "fact_orders")

# ── Write Silver ──────────────────────────────────────────────────────────────
#
# Partition by order_year and order_month. Overwrite mode replaces previous
# output, which is correct for a full CDC reconciliation run.

silver_path = paths.silver_table("fact_orders")
print(f"[fact_orders] Writing Silver to {silver_path}")
clean_df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(silver_path)
print("[fact_orders] Done.")

commit_job(job)
