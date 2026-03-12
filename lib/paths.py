"""
Path resolution for Bronze, Silver, and Quarantine locations.

The Glue jobs don't hardcode any paths. Instead, they receive three parameters
at runtime: --BRONZE_PATH, --SILVER_PATH, and --QUARANTINE_PATH.

Locally these are file:// paths pointing to data/ inside the Docker container.
In AWS they are s3:// paths pointing to the real S3 buckets.

The job code never needs to know which environment it's in — the only thing
that changes between local and AWS is the value of these three parameters.

Local example:
    --BRONZE_PATH    file:///home/glue_user/workspace/data/bronze
    --SILVER_PATH    file:///home/glue_user/workspace/data/silver
    --QUARANTINE_PATH file:///home/glue_user/workspace/data/quarantine

AWS example:
    --BRONZE_PATH    s3://edp-dev-123456789012-bronze
    --SILVER_PATH    s3://edp-dev-123456789012-silver
    --QUARANTINE_PATH s3://edp-dev-123456789012-quarantine
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    """Resolved paths for a single Glue job run."""

    bronze_root: str
    silver_root: str
    quarantine_root: str

    def bronze_table(self, table: str) -> str:
        """
        Full path to a Bronze table's directory.

        DMS writes to raw/public/<table>/ inside the bronze bucket.
        We read the whole directory so we pick up both the initial
        LOAD file and all subsequent CDC date-partitioned files.

        Example:
            file:///home/glue_user/workspace/data/bronze/raw/public/customers
        """
        return f"{self.bronze_root}/raw/public/{table}"

    def silver_table(self, table: str) -> str:
        """
        Full path to a Silver table's output directory.

        Example:
            file:///home/glue_user/workspace/data/silver/dim_customer
        """
        return f"{self.silver_root}/{table}"

    def quarantine_table(self, table: str) -> str:
        """
        Full path to the quarantine output directory for a table.

        Example:
            file:///home/glue_user/workspace/data/quarantine/dim_customer
        """
        return f"{self.quarantine_root}/{table}"


def resolve_paths(args: dict) -> Paths:
    """
    Build a Paths instance from Glue job arguments.

    Call this at the top of every job after getResolvedOptions().
    """
    return Paths(
        bronze_root=args["BRONZE_PATH"],
        silver_root=args["SILVER_PATH"],
        quarantine_root=args["QUARANTINE_PATH"],
    )
