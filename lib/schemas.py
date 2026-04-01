"""
Spark schema definitions for the Bronze tables.

DMS adds two extra columns to every row it writes:
  - Op STRING: the operation type: 'I' (insert), 'U' (update), 'D' (delete).
    Even full-load files have Op='I' on every row because we set
    include_op_for_full_load=true in the DMS endpoint configuration.
  - _dms_timestamp TIMESTAMP: when DMS wrote this row. Used for CDC reconciliation:
    we sort by this column to determine which version of a row is latest.

These two columns appear first, followed by the original table columns in their
PostgreSQL definition order.

We pass explicit schemas to spark.read.parquet() rather than relying on schema
inference. This is faster (no extra scan pass) and safer (types are guaranteed to
match what the CDC reconciliation code expects).
"""

from __future__ import annotations

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Shared DMS metadata columns ───────────────────────────────────────────────

# DMS writes _dms_timestamp as a plain string ("2026-03-11 13:07:07.797094"),
# not as a Parquet timestamp type. The format is ISO 8601-compatible so
# lexicographic ordering in cdc.reconcile() still produces the correct result.
_DMS_FIELDS = [
    StructField("Op", StringType(), nullable=True),
    StructField("_dms_timestamp", StringType(), nullable=True),
]

# ── Table schemas ─────────────────────────────────────────────────────────────

CUSTOMERS_SCHEMA = StructType(
    _DMS_FIELDS
    + [
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("phone", StringType(), nullable=True),
        StructField("signup_date", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

PRODUCTS_SCHEMA = StructType(
    _DMS_FIELDS
    + [
        StructField("product_id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("brand", StringType(), nullable=False),
        StructField("unit_price", DecimalType(10, 2), nullable=False),
        StructField("stock_qty", IntegerType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

ORDERS_SCHEMA = StructType(
    _DMS_FIELDS
    + [
        StructField("order_id", IntegerType(), nullable=False),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("order_date", TimestampType(), nullable=False),
        StructField("order_status", StringType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

ORDER_ITEMS_SCHEMA = StructType(
    _DMS_FIELDS
    + [
        StructField("order_item_id", IntegerType(), nullable=False),
        StructField("order_id", IntegerType(), nullable=False),
        StructField("product_id", IntegerType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DecimalType(10, 2), nullable=False),
        StructField("line_total", DecimalType(10, 2), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

PAYMENTS_SCHEMA = StructType(
    _DMS_FIELDS
    + [
        StructField("payment_id", IntegerType(), nullable=False),
        StructField("order_id", IntegerType(), nullable=False),
        StructField("method", StringType(), nullable=False),
        StructField("amount", DecimalType(10, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("payment_date", TimestampType(), nullable=False),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)

SHIPMENTS_SCHEMA = StructType(
    _DMS_FIELDS
    + [
        StructField("shipment_id", IntegerType(), nullable=False),
        StructField("order_id", IntegerType(), nullable=False),
        StructField("carrier", StringType(), nullable=False),
        StructField("delivery_status", StringType(), nullable=False),
        StructField("shipped_date", TimestampType(), nullable=True),
        StructField("delivered_date", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=False),
    ]
)
