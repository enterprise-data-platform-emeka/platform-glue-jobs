"""
Unit tests for lib/paths.py.

These tests run on the host machine without Docker or Spark.
lib/paths.py is pure Python so no Glue dependencies are needed.
"""

import sys
from pathlib import Path

# Make lib/ importable when running pytest from the project root.
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.paths import Paths, resolve_paths  # noqa: E402


class TestPaths:
    def test_bronze_table_local(self):
        p = Paths(
            bronze_root="file:///workspace/data/bronze",
            silver_root="file:///workspace/data/silver",
            quarantine_root="file:///workspace/data/quarantine",
        )
        assert p.bronze_table("customers") == (
            "file:///workspace/data/bronze/raw/public/customers"
        )

    def test_bronze_table_aws(self):
        p = Paths(
            bronze_root="s3://edp-dev-123456789012-bronze",
            silver_root="s3://edp-dev-123456789012-silver",
            quarantine_root="s3://edp-dev-123456789012-quarantine",
        )
        assert p.bronze_table("orders") == (
            "s3://edp-dev-123456789012-bronze/raw/public/orders"
        )

    def test_silver_table(self):
        p = Paths(
            bronze_root="file:///b",
            silver_root="file:///s",
            quarantine_root="file:///q",
        )
        assert p.silver_table("dim_customer") == "file:///s/dim_customer"

    def test_quarantine_table(self):
        p = Paths(
            bronze_root="file:///b",
            silver_root="file:///s",
            quarantine_root="file:///q",
        )
        assert p.quarantine_table("dim_customer") == "file:///q/dim_customer"

    def test_paths_are_immutable(self):
        p = Paths(
            bronze_root="file:///b",
            silver_root="file:///s",
            quarantine_root="file:///q",
        )
        try:
            p.bronze_root = "something_else"
            assert False, "Should have raised FrozenInstanceError"
        except Exception:
            pass  # frozen=True means assignment raises an exception


class TestResolvePaths:
    def test_resolve_from_args(self):
        args = {
            "BRONZE_PATH": "s3://my-bronze-bucket",
            "SILVER_PATH": "s3://my-silver-bucket",
            "QUARANTINE_PATH": "s3://my-quarantine-bucket",
        }
        paths = resolve_paths(args)
        assert paths.bronze_root == "s3://my-bronze-bucket"
        assert paths.silver_root == "s3://my-silver-bucket"
        assert paths.quarantine_root == "s3://my-quarantine-bucket"

    def test_resolve_local_paths(self):
        args = {
            "BRONZE_PATH": "file:///home/glue_user/workspace/data/bronze",
            "SILVER_PATH": "file:///home/glue_user/workspace/data/silver",
            "QUARANTINE_PATH": "file:///home/glue_user/workspace/data/quarantine",
        }
        paths = resolve_paths(args)
        assert paths.bronze_table("customers").startswith("file://")
        assert "raw/public/customers" in paths.bronze_table("customers")
