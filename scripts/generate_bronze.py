"""
Generate local Bronze Parquet test data that exactly mirrors what AWS DMS produces.

DMS writes two extra columns to every row:
  - Op             — operation type: 'I' (insert), 'U' (update), 'D' (delete).
                     Full-load files use Op='I' for every row because the DMS
                     endpoint is configured with include_op_for_full_load=true.
  - _dms_timestamp — timestamp when DMS wrote this row. Used by cdc.reconcile()
                     to determine which version of a row is the most recent.

File layout produced:
    data/bronze/raw/public/customers/LOAD00000001.parquet
    data/bronze/raw/public/products/LOAD00000001.parquet
    data/bronze/raw/public/orders/LOAD00000001.parquet
    data/bronze/raw/public/order_items/LOAD00000001.parquet
    data/bronze/raw/public/payments/LOAD00000001.parquet
    data/bronze/raw/public/shipments/LOAD00000001.parquet

This is the full-load snapshot only. For CDC testing a separate script would
generate date-partitioned files with Op='U' and Op='D' rows.

Run: python scripts/generate_bronze.py
Output: data/bronze/raw/public/<table>/LOAD00000001.parquet
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from faker import Faker

# ── Configuration ─────────────────────────────────────────────────────────────

SEED = 42
N_CUSTOMERS = 50
N_PRODUCTS = 20
N_ORDERS = 150
MAX_ITEMS_PER_ORDER = 4

BRONZE_ROOT = Path(__file__).parent.parent / "data" / "bronze" / "raw" / "public"

ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
ORDER_STATUS_WEIGHTS = [5, 15, 20, 50, 10]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer"]
PAYMENT_WEIGHTS = [40, 25, 25, 10]

PAYMENT_STATUSES = ["completed", "pending", "failed", "refunded"]
PAYMENT_STATUS_WEIGHTS = [80, 5, 5, 10]

CARRIERS = ["DHL", "FedEx", "UPS", "Royal Mail", "DPD"]
CARRIER_WEIGHTS = [30, 25, 20, 15, 10]

DELIVERY_STATUSES = ["pending", "in_transit", "delivered", "failed"]
DELIVERY_STATUS_WEIGHTS = [10, 20, 65, 5]

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports & Outdoors", "Books"]
CATEGORY_WEIGHTS = [30, 25, 20, 15, 10]

BRANDS = ["TechPro", "StyleCo", "HomeMax", "SportElite", "ReadMore",
          "NovaTech", "UrbanStyle", "GardenPro"]

COUNTRIES = ["Germany", "France", "UK", "Netherlands", "Spain",
             "Italy", "Poland", "Sweden", "Belgium", "Austria"]
COUNTRY_WEIGHTS = [20, 15, 15, 10, 10, 10, 5, 5, 5, 5]

# ── Helpers ───────────────────────────────────────────────────────────────────

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

_now = datetime(2024, 6, 1, tzinfo=UTC)


def _ts(days_ago: float = 0.0) -> datetime:
    """Return a UTC timestamp offset from the reference date."""
    return _now - timedelta(days=days_ago)  # type: ignore[return-value]


def _dms_ts(days_ago: float = 0.0) -> str:
    """Simulate a _dms_timestamp slightly after the row's own timestamp.

    Returns a plain string in the same format DMS uses: 'YYYY-MM-DD HH:MM:SS.ffffff'.
    DMS writes this column as Parquet string, not as a timestamp type.
    The format is ISO 8601-compatible so lexicographic ordering works correctly
    in cdc.reconcile() when finding the latest version of each row.
    """
    ts = _ts(days_ago) + timedelta(seconds=random.uniform(1, 60))
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")


def _write(table_name: str, df: pd.DataFrame) -> None:
    """Write a DataFrame to data/bronze/raw/public/<table>/LOAD00000001.parquet.

    Type casts applied to match real AWS DMS Parquet output exactly:
      - int64  → int32         (DMS maps PostgreSQL INTEGER to Parquet INT32)
      - float64 → decimal128(10,2)  (DMS maps PostgreSQL NUMERIC to Parquet DECIMAL)
      - _dms_timestamp is already a string (written by _dms_ts())
    """
    out_dir = BRONZE_ROOT / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "LOAD00000001.parquet"
    for col in df.select_dtypes(include="int64").columns:
        df[col] = df[col].astype("int32")
    table = pa.Table.from_pandas(df, preserve_index=False)
    for i in range(table.num_columns):
        if pa.types.is_floating(table.schema.field(i).type):
            table = table.set_column(
                i,
                table.schema.field(i).with_type(pa.decimal128(10, 2)),
                pc.cast(table.column(i), pa.decimal128(10, 2)),
            )
    pq.write_table(table, out_path, compression="gzip")
    print(f"  Wrote {len(df):>5} rows -> {out_path.relative_to(Path.cwd())}")


# ── Generate data ─────────────────────────────────────────────────────────────

def generate_customers() -> pd.DataFrame:
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        days_ago = random.uniform(30, 730)
        signup = _ts(days_ago)
        updated = signup + timedelta(days=random.uniform(0, days_ago))
        rows.append({
            "Op": "I",
            "_dms_timestamp": _dms_ts(days_ago),
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"{fake.user_name()}.{i}@{random.choice(['gmail.com','outlook.com','yahoo.com'])}",
            "country": random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0],
            "phone": fake.phone_number(),
            "signup_date": signup,
            "updated_at": updated,
        })
    return pd.DataFrame(rows)


def generate_products() -> pd.DataFrame:
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS)[0]
        brand = random.choice(BRANDS)
        adj = random.choice(["Pro", "Ultra", "Lite", "Max", "Plus", "Elite"])
        noun = category.split("&")[0].strip().rstrip("s")
        days_ago = random.uniform(60, 365)
        rows.append({
            "Op": "I",
            "_dms_timestamp": _dms_ts(days_ago),
            "product_id": i,
            "name": f"{brand} {adj} {noun}",
            "category": category,
            "brand": brand,
            "unit_price": round(random.uniform(5.0, 500.0), 2),
            "stock_qty": random.randint(0, 200),
            "updated_at": _ts(days_ago),
        })
    return pd.DataFrame(rows)


def generate_orders(customer_ids: list[int]) -> pd.DataFrame:
    rows = []
    for i in range(1, N_ORDERS + 1):
        days_ago = random.uniform(1, 365)
        order_date = _ts(days_ago)
        status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0]
        rows.append({
            "Op": "I",
            "_dms_timestamp": _dms_ts(days_ago),
            "order_id": i,
            "customer_id": random.choice(customer_ids),
            "order_date": order_date,
            "order_status": status,
            "updated_at": order_date + timedelta(hours=random.uniform(0, 48)),
        })
    return pd.DataFrame(rows)


def generate_order_items(order_ids: list[int], product_ids: list[int],
                         products_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    item_id = 1
    # Build a quick price lookup: product_id -> unit_price
    price_lookup = dict(zip(products_df["product_id"], products_df["unit_price"]))
    for order_id in order_ids:
        n_items = random.randint(1, MAX_ITEMS_PER_ORDER)
        chosen_products = random.sample(product_ids, min(n_items, len(product_ids)))
        for product_id in chosen_products:
            unit_price = float(price_lookup[product_id])
            qty = random.randint(1, 5)
            rows.append({
                "Op": "I",
                "_dms_timestamp": _dms_ts(random.uniform(1, 365)),
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": qty,
                "unit_price": round(unit_price, 2),
                "line_total": round(qty * unit_price, 2),
                "updated_at": _ts(random.uniform(1, 365)),
            })
            item_id += 1
    return pd.DataFrame(rows)


def generate_payments(order_ids: list[int], orders_df: pd.DataFrame) -> pd.DataFrame:
    """One payment per order."""
    rows = []
    # Build order_id -> order_date lookup
    date_lookup = dict(zip(orders_df["order_id"], orders_df["order_date"]))
    for i, order_id in enumerate(order_ids, start=1):
        order_date = date_lookup[order_id]
        payment_date = order_date + timedelta(hours=random.uniform(0, 2))
        rows.append({
            "Op": "I",
            "_dms_timestamp": _dms_ts(random.uniform(1, 365)),
            "payment_id": i,
            "order_id": order_id,
            "method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "status": random.choices(PAYMENT_STATUSES, weights=PAYMENT_STATUS_WEIGHTS)[0],
            "payment_date": payment_date,
            "updated_at": payment_date + timedelta(minutes=random.uniform(0, 30)),
        })
    return pd.DataFrame(rows)


def generate_shipments(order_ids: list[int], orders_df: pd.DataFrame) -> pd.DataFrame:
    """One shipment per order (only for shipped/delivered orders)."""
    rows = []
    date_lookup = dict(zip(orders_df["order_id"], orders_df["order_date"]))
    status_lookup = dict(zip(orders_df["order_id"], orders_df["order_status"]))
    shipment_id = 1
    for order_id in order_ids:
        order_status = status_lookup[order_id]
        # Only create shipments for orders that have actually shipped
        if order_status not in ("shipped", "delivered"):
            continue
        order_date = date_lookup[order_id]
        shipped_date = order_date + timedelta(days=random.uniform(1, 3))
        delivery_status = random.choices(DELIVERY_STATUSES, weights=DELIVERY_STATUS_WEIGHTS)[0]
        delivered_date = (
            shipped_date + timedelta(days=random.uniform(1, 7))
            if delivery_status == "delivered"
            else None
        )
        rows.append({
            "Op": "I",
            "_dms_timestamp": _dms_ts(random.uniform(1, 365)),
            "shipment_id": shipment_id,
            "order_id": order_id,
            "carrier": random.choices(CARRIERS, weights=CARRIER_WEIGHTS)[0],
            "delivery_status": delivery_status,
            "shipped_date": shipped_date,
            "delivered_date": delivered_date,
            "updated_at": shipped_date + timedelta(hours=random.uniform(0, 1)),
        })
        shipment_id += 1
    return pd.DataFrame(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Generating Bronze test data...")
    print()

    customers_df = generate_customers()
    _write("customers", customers_df)

    products_df = generate_products()
    _write("products", products_df)

    orders_df = generate_orders(customers_df["customer_id"].tolist())
    _write("orders", orders_df)

    order_items_df = generate_order_items(
        orders_df["order_id"].tolist(),
        products_df["product_id"].tolist(),
        products_df,
    )
    _write("order_items", order_items_df)

    payments_df = generate_payments(orders_df["order_id"].tolist(), orders_df)
    _write("payments", payments_df)

    shipments_df = generate_shipments(orders_df["order_id"].tolist(), orders_df)
    _write("shipments", shipments_df)

    print()
    print("Bronze data ready. Run 'make run-all' to execute the Silver jobs.")


if __name__ == "__main__":
    main()
