"""
Silver job: fact_order_items

Reads the Bronze order_items table, reconciles CDC operations to get the
current state of every line item, enriches each row with the order_date from
the Bronze orders table (used for partitioning), validates data quality, and
writes the clean fact table to Silver.

Input (Bronze):
    {BRONZE_PATH}/raw/public/order_items/
    Columns: Op, _dms_timestamp, order_item_id, order_id, product_id,
             quantity, unit_price, line_total, updated_at

    {BRONZE_PATH}/raw/public/orders/
    Columns: Op, _dms_timestamp, order_id, order_date, ...
    (used only to pull order_date for partitioning)

Output (Silver):
    {SILVER_PATH}/fact_order_items/
    Columns: order_item_id, order_id, product_id, quantity, unit_price,
             line_total, order_year, order_month
    Partitioned by: order_year, order_month

Quarantine:
    {QUARANTINE_PATH}/fact_order_items/
    Rows that failed validation, plus a _validation_errors column.

Validation rules:
    - order_item_id must not be null
    - order_id must not be null
    - product_id must not be null
    - quantity must be greater than zero
    - unit_price must be greater than zero
    - line_total must be greater than zero

Why join to orders for order_date:
    fact_order_items doesn't have its own date. Partitioning by order_date
    (from the orders table) means that a query like "show me all line items
    for orders placed in January 2024" only scans the January 2024 partition,
    not the full table. This is the same partitioning strategy as fact_orders,
    so both tables are aligned and Athena can push down the partition filter
    consistently across joins.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from lib.cdc import reconcile
from lib.paths import resolve_paths
from lib.schemas import ORDER_ITEMS_SCHEMA, ORDERS_SCHEMA
from lib.validation import validate

# ── Job initialisation ────────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "BRONZE_PATH", "SILVER_PATH", "QUARANTINE_PATH"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

paths = resolve_paths(args)

# ── Read Bronze: order_items ──────────────────────────────────────────────────

print(f"[fact_order_items] Reading Bronze order_items from {paths.bronze_table('order_items')}")
bronze_items_df = spark.read.schema(ORDER_ITEMS_SCHEMA).parquet(
    paths.bronze_table("order_items")
)
print(f"[fact_order_items] Bronze order_items row count: {bronze_items_df.count()}")

# ── Read Bronze: orders (for order_date) ──────────────────────────────────────
#
# We only need order_id and order_date from orders. Reuse the schema but
# select only what we need before the join to keep memory usage down.

print(f"[fact_order_items] Reading Bronze orders from {paths.bronze_table('orders')}")
bronze_orders_df = (
    spark.read.schema(ORDERS_SCHEMA)
    .parquet(paths.bronze_table("orders"))
    .select("Op", "_dms_timestamp", "order_id", "order_date")
)

# ── CDC reconciliation ────────────────────────────────────────────────────────

current_items_df = reconcile(bronze_items_df, pk_col="order_item_id")
print(f"[fact_order_items] After CDC reconciliation (items): {current_items_df.count()} rows")

# Reconcile orders too so we only join against currently-live orders.
current_orders_df = reconcile(bronze_orders_df, pk_col="order_id").select(
    "order_id", "order_date"
)

# ── Build fact table columns ──────────────────────────────────────────────────
#
# Join order_items to orders to get order_date, then derive year/month
# partition columns. Drop updated_at — it's a technical audit column.

fact_df = (
    current_items_df
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
    )
    .join(current_orders_df, on="order_id", how="left")
    .withColumn("order_year", F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    .drop("order_date")
)

# ── Validate ──────────────────────────────────────────────────────────────────

RULES = {
    "order_item_id_not_null": "order_item_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
    "product_id_not_null": "product_id IS NOT NULL",
    "quantity_positive": "quantity > 0",
    "unit_price_positive": "unit_price > 0",
    "line_total_positive": "line_total > 0",
}

clean_df = validate(fact_df, RULES, paths.quarantine_root, "fact_order_items")
print(f"[fact_order_items] Valid rows after validation: {clean_df.count()}")

# ── Write Silver ──────────────────────────────────────────────────────────────

silver_path = paths.silver_table("fact_order_items")
print(f"[fact_order_items] Writing Silver to {silver_path}")
clean_df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(silver_path)
print(f"[fact_order_items] Done.")

job.commit()
