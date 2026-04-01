"""
Silver job: fact_shipments

Reads the Bronze shipments table, reconciles CDC operations to get the current
state of every shipment, validates data quality, and writes the clean fact table
to Silver partitioned by shipped date.

One shipment exists per order. Shipments track fulfilment: when an order left
the warehouse, when it was delivered, and whether the delivery succeeded.

Input (Bronze):
    {BRONZE_PATH}/raw/public/shipments/
    Columns: Op, _dms_timestamp, shipment_id, order_id, carrier,
             delivery_status, shipped_date, delivered_date, updated_at

Output (Silver):
    {SILVER_PATH}/fact_shipments/
    Columns: shipment_id, order_id, carrier, delivery_status,
             shipped_date, delivered_date, delivery_days,
             shipped_year, shipped_month
    Partitioned by: shipped_year, shipped_month

Quarantine:
    {QUARANTINE_PATH}/fact_shipments/
    Rows that failed validation, plus a _validation_errors column.

Validation rules:
    - shipment_id must not be null
    - order_id must not be null
    - carrier must not be null
    - delivery_status must not be null
    - shipped_date must not be null (a shipment without a shipped_date is meaningless)
    - shipped_year must not be null (guards against null shipped_date propagation)
    - shipped_month must not be null
    - delivery_days must be >= 0 when not null (delivered_date >= shipped_date)

Derived column, delivery_days:
    The number of calendar days between shipped_date and delivered_date.
    Null when the shipment has not yet been delivered (delivery_status != 'delivered').
    This is a pre-computed metric rather than a raw timestamp pair so that dbt
    and Athena don't have to recompute the date diff in every query. It's the
    primary input for "average delivery time by carrier" in the Gold layer.

Business questions this fact table enables (in Gold via dbt/Athena):
    - Average delivery time by carrier (AVG(delivery_days) GROUP BY carrier)
    - On-time delivery rate (if a target SLA is known)
    - Delivery failure rate by carrier (COUNT WHERE status='failed' / COUNT total)
    - Fulfilment volume by month (COUNT shipped_date by month)
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
from lib.schemas import SHIPMENTS_SCHEMA
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

print(f"[fact_shipments] Reading Bronze from {paths.bronze_table('shipments')}")
bronze_df = spark.read.schema(SHIPMENTS_SCHEMA).parquet(paths.bronze_table("shipments"))

# ── CDC reconciliation ────────────────────────────────────────────────────────
#
# Shipments are updated as they move through the delivery pipeline:
# pending -> in_transit -> delivered (or failed). CDC reconciliation keeps
# only the latest status for each shipment_id.

current_df = reconcile(bronze_df, pk_col="shipment_id")

# ── Build fact table columns ──────────────────────────────────────────────────
#
# Compute delivery_days as a pre-aggregated metric. datediff() returns an
# integer (number of whole calendar days). It returns null when delivered_date
# is null (i.e. the shipment has not been delivered yet), which is correct.
# we do not want to impute a delivery time for in-flight shipments.
#
# Partition by shipped_year and shipped_month so Athena can push down a date
# filter and scan only the relevant months.

fact_df = current_df.select(
    "shipment_id",
    "order_id",
    "carrier",
    "delivery_status",
    "shipped_date",
    "delivered_date",
    F.datediff(F.col("delivered_date"), F.col("shipped_date")).alias("delivery_days"),
    F.year("shipped_date").alias("shipped_year"),
    F.month("shipped_date").alias("shipped_month"),
)

# ── Validate ──────────────────────────────────────────────────────────────────

RULES = {
    "shipment_id_not_null": "shipment_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
    "carrier_not_null": "carrier IS NOT NULL",
    "delivery_status_not_null": "delivery_status IS NOT NULL",
    # shipped_date is the anchor date for this fact table. A shipment record
    # with no shipped_date has no meaningful place in a time-partitioned table.
    "shipped_date_not_null": "shipped_date IS NOT NULL",
    # Guard against null partition values.
    "shipped_year_not_null": "shipped_year IS NOT NULL",
    "shipped_month_not_null": "shipped_month IS NOT NULL",
    # delivery_days must be non-negative when present. A negative value means
    # delivered_date < shipped_date, which is physically impossible and indicates
    # a data entry error in the source system.
    "delivery_days_non_negative": "delivery_days IS NULL OR delivery_days >= 0",
}

clean_df = validate(fact_df, RULES, paths.quarantine_root, "fact_shipments")

# ── Write Silver ──────────────────────────────────────────────────────────────

silver_path = paths.silver_table("fact_shipments")
print(f"[fact_shipments] Writing Silver to {silver_path}")
clean_df.write.mode("overwrite").partitionBy("shipped_year", "shipped_month").parquet(silver_path)
print("[fact_shipments] Done.")

commit_job(job)
