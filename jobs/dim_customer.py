"""
Silver job: dim_customer

Reads the Bronze customers table, reconciles CDC operations to get the current
state of every customer, validates data quality, and writes the clean dimension
table to Silver.

Input (Bronze):
    {BRONZE_PATH}/raw/public/customers/
    Columns: Op, _dms_timestamp, customer_id, first_name, last_name, email,
             country, phone, signup_date, updated_at

Output (Silver):
    {SILVER_PATH}/dim_customer/
    Columns: customer_id, first_name, last_name, email, country, phone, signup_date

Quarantine:
    {QUARANTINE_PATH}/dim_customer/
    Rows that failed validation, plus a _validation_errors column.

Validation rules:
    - customer_id must not be null
    - email must not be null
    - country must not be null
    - first_name must not be null
    - last_name must not be null

Why updated_at is dropped:
    updated_at is a technical audit column from PostgreSQL. It tells us when the
    row was last changed in the source system, which is already captured by
    _dms_timestamp at the Bronze layer. The Silver dimension table is about the
    business attributes of a customer, not internal change tracking.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from lib.cdc import reconcile
from lib.job_utils import commit_job, init_job
from lib.paths import resolve_paths
from lib.schemas import CUSTOMERS_SCHEMA
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
#
# Reading the entire directory picks up both the initial LOAD file and any
# date-partitioned CDC files written since the full load completed.

print(f"[dim_customer] Reading Bronze from {paths.bronze_table('customers')}")
bronze_df = spark.read.schema(CUSTOMERS_SCHEMA).parquet(paths.bronze_table("customers"))

# ── CDC reconciliation ────────────────────────────────────────────────────────
#
# Resolves all Insert / Update / Delete operations into the current state.
# Each customer_id ends up with exactly one row reflecting its latest values.

current_df = reconcile(bronze_df, pk_col="customer_id")

# ── Select Silver columns ─────────────────────────────────────────────────────
#
# Drop updated_at, it's a technical audit column we don't need in Silver.
# The business columns are what matter in the dimension table.

dim_df = current_df.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country",
    "phone",
    "signup_date",
)

# ── Validate ──────────────────────────────────────────────────────────────────

RULES = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "first_name_not_null": "first_name IS NOT NULL",
    "last_name_not_null": "last_name IS NOT NULL",
    "email_not_null": "email IS NOT NULL",
    "country_not_null": "country IS NOT NULL",
}

clean_df = validate(dim_df, RULES, paths.quarantine_root, "dim_customer")

# ── Write Silver ──────────────────────────────────────────────────────────────
#
# Overwrite mode replaces the previous run's output. dim_customer is a full
# snapshot of the current state, there's no concept of incremental appends here.

silver_path = paths.silver_table("dim_customer")
print(f"[dim_customer] Writing Silver to {silver_path}")
clean_df.write.mode("overwrite").parquet(silver_path)
print("[dim_customer] Done.")

commit_job(job)
