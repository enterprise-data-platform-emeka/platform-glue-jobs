"""
Silver job: dim_product

Reads the Bronze products table, reconciles CDC operations to get the current
state of every product, validates data quality, and writes the clean dimension
table to Silver.

Input (Bronze):
    {BRONZE_PATH}/raw/public/products/
    Columns: Op, _dms_timestamp, product_id, name, category, brand,
             unit_price, stock_qty, updated_at

Output (Silver):
    {SILVER_PATH}/dim_product/
    Columns: product_id, name, category, brand, unit_price, stock_qty

Quarantine:
    {QUARANTINE_PATH}/dim_product/
    Rows that failed validation, plus a _validation_errors column.

Validation rules:
    - product_id must not be null
    - name must not be null
    - category must not be null
    - brand must not be null
    - unit_price must be greater than zero
    - stock_qty must be greater than or equal to zero

Note on stock_qty:
    stock_qty reflects the current stock level in the source OLTP system.
    It changes frequently as orders arrive. The CDC reconciliation ensures
    the Silver table always shows the latest value, not the value at the
    time of the initial load.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from lib.cdc import reconcile
from lib.paths import resolve_paths
from lib.schemas import PRODUCTS_SCHEMA
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

# ── Read Bronze ───────────────────────────────────────────────────────────────

print(f"[dim_product] Reading Bronze from {paths.bronze_table('products')}")
bronze_df = spark.read.schema(PRODUCTS_SCHEMA).parquet(paths.bronze_table("products"))

# ── CDC reconciliation ────────────────────────────────────────────────────────

current_df = reconcile(bronze_df, pk_col="product_id")

# ── Select Silver columns ─────────────────────────────────────────────────────

dim_df = current_df.select(
    "product_id",
    "name",
    "category",
    "brand",
    "unit_price",
    "stock_qty",
)

# ── Validate ──────────────────────────────────────────────────────────────────

RULES = {
    "product_id_not_null": "product_id IS NOT NULL",
    "name_not_null": "name IS NOT NULL",
    "category_not_null": "category IS NOT NULL",
    "brand_not_null": "brand IS NOT NULL",
    "unit_price_positive": "unit_price > 0",
    "stock_qty_non_negative": "stock_qty >= 0",
}

clean_df = validate(dim_df, RULES, paths.quarantine_root, "dim_product")

# ── Write Silver ──────────────────────────────────────────────────────────────

silver_path = paths.silver_table("dim_product")
print(f"[dim_product] Writing Silver to {silver_path}")
clean_df.write.mode("overwrite").parquet(silver_path)
print("[dim_product] Done.")

job.commit()
