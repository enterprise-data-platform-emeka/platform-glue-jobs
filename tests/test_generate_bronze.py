"""
Unit tests for scripts/generate_bronze.py.

Tests verify that the generator produces DataFrames with the correct columns,
correct Op values, non-null primary keys, and referential integrity between
tables. These tests run on the host without Docker or Spark.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.generate_bronze import (  # noqa: E402
    generate_customers,
    generate_order_items,
    generate_orders,
    generate_payments,
    generate_products,
    generate_shipments,
)


class TestGenerateCustomers:
    def setup_method(self):
        self.df = generate_customers()

    def test_row_count(self):
        from scripts.generate_bronze import N_CUSTOMERS
        assert len(self.df) == N_CUSTOMERS

    def test_op_is_insert(self):
        assert (self.df["Op"] == "I").all()

    def test_has_dms_timestamp(self):
        assert "_dms_timestamp" in self.df.columns

    def test_customer_id_unique(self):
        assert self.df["customer_id"].nunique() == len(self.df)

    def test_required_columns_not_null(self):
        for col in ["customer_id", "first_name", "last_name", "email", "country"]:
            assert self.df[col].notna().all(), f"{col} has nulls"


class TestGenerateProducts:
    def setup_method(self):
        self.df = generate_products()

    def test_row_count(self):
        from scripts.generate_bronze import N_PRODUCTS
        assert len(self.df) == N_PRODUCTS

    def test_op_is_insert(self):
        assert (self.df["Op"] == "I").all()

    def test_product_id_unique(self):
        assert self.df["product_id"].nunique() == len(self.df)

    def test_unit_price_positive(self):
        assert (self.df["unit_price"] > 0).all()

    def test_stock_qty_non_negative(self):
        assert (self.df["stock_qty"] >= 0).all()


class TestGenerateOrders:
    def setup_method(self):
        self.customers_df = generate_customers()
        self.orders_df = generate_orders(self.customers_df["customer_id"].tolist())

    def test_row_count(self):
        from scripts.generate_bronze import N_ORDERS
        assert len(self.orders_df) == N_ORDERS

    def test_op_is_insert(self):
        assert (self.orders_df["Op"] == "I").all()

    def test_order_id_unique(self):
        assert self.orders_df["order_id"].nunique() == len(self.orders_df)

    def test_customer_id_is_valid(self):
        valid_ids = set(self.customers_df["customer_id"])
        assert self.orders_df["customer_id"].isin(valid_ids).all()

    def test_order_date_not_null(self):
        assert self.orders_df["order_date"].notna().all()


class TestGenerateOrderItems:
    def setup_method(self):
        self.customers_df = generate_customers()
        self.products_df = generate_products()
        self.orders_df = generate_orders(self.customers_df["customer_id"].tolist())
        self.items_df = generate_order_items(
            self.orders_df["order_id"].tolist(),
            self.products_df["product_id"].tolist(),
            self.products_df,
        )

    def test_op_is_insert(self):
        assert (self.items_df["Op"] == "I").all()

    def test_order_item_id_unique(self):
        assert self.items_df["order_item_id"].nunique() == len(self.items_df)

    def test_order_ids_are_valid(self):
        valid_ids = set(self.orders_df["order_id"])
        assert self.items_df["order_id"].isin(valid_ids).all()

    def test_product_ids_are_valid(self):
        valid_ids = set(self.products_df["product_id"])
        assert self.items_df["product_id"].isin(valid_ids).all()

    def test_line_total_matches_qty_price(self):
        expected = (self.items_df["quantity"] * self.items_df["unit_price"]).round(2)
        actual = self.items_df["line_total"].round(2)
        assert (expected == actual).all()

    def test_quantity_positive(self):
        assert (self.items_df["quantity"] > 0).all()


class TestGeneratePayments:
    def setup_method(self):
        self.customers_df = generate_customers()
        self.orders_df = generate_orders(self.customers_df["customer_id"].tolist())
        self.payments_df = generate_payments(
            self.orders_df["order_id"].tolist(), self.orders_df
        )

    def test_op_is_insert(self):
        assert (self.payments_df["Op"] == "I").all()

    def test_one_payment_per_order(self):
        assert len(self.payments_df) == len(self.orders_df)

    def test_order_ids_are_valid(self):
        valid_ids = set(self.orders_df["order_id"])
        assert self.payments_df["order_id"].isin(valid_ids).all()

    def test_amount_positive(self):
        assert (self.payments_df["amount"] > 0).all()


class TestGenerateShipments:
    def setup_method(self):
        self.customers_df = generate_customers()
        self.orders_df = generate_orders(self.customers_df["customer_id"].tolist())
        self.shipments_df = generate_shipments(
            self.orders_df["order_id"].tolist(), self.orders_df
        )

    def test_op_is_insert(self):
        assert (self.shipments_df["Op"] == "I").all()

    def test_only_shipped_or_delivered_orders_have_shipments(self):
        valid_statuses = {"shipped", "delivered"}
        status_lookup = dict(
            zip(self.orders_df["order_id"], self.orders_df["order_status"])
        )
        for order_id in self.shipments_df["order_id"]:
            assert status_lookup[order_id] in valid_statuses

    def test_shipment_id_unique(self):
        assert self.shipments_df["shipment_id"].nunique() == len(self.shipments_df)
