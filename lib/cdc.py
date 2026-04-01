"""
CDC (Change Data Capture) reconciliation.

DMS writes two kinds of files to the Bronze S3 bucket:

  1. Full-load file: LOAD00000001.parquet
     Written once when DMS first copies the entire table. Every row has Op='I'.
     This is the starting state.

  2. CDC files: raw/public/<table>/YYYY/MM/DD/YYYYMMDD-HHmmss-ttt.parquet
     Written continuously as rows change in PostgreSQL. Each file contains a mix
     of Op='I' (new rows), Op='U' (updated rows), and Op='D' (deleted rows).

To reconstruct the current state of a table we:
  1. Read ALL files for the table (LOAD + all CDC date-partitioned files).
  2. Sort by _dms_timestamp ascending so earlier events come first.
  3. For each primary key, keep only the most recent row (window function).
  4. Discard rows where the most recent operation was 'D' (the row was deleted).
  5. Drop the Op and _dms_timestamp columns, as downstream jobs don't need them.

The result is a clean snapshot of every row that currently exists in the source
table, identical to what you'd get from SELECT * on the live PostgreSQL.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def reconcile(df: DataFrame, pk_col: str) -> DataFrame:
    """
    Resolve a Bronze CDC DataFrame into its current state.

    Parameters
    ----------
    df:
        Raw Bronze DataFrame with Op and _dms_timestamp columns.
    pk_col:
        Name of the primary key column (e.g. 'customer_id').

    Returns
    -------
    DataFrame with Op and _dms_timestamp removed, containing only the
    currently-live rows (deletes removed).

    Example
    -------
    Given these Bronze rows for customer_id=42:

        Op  _dms_timestamp          customer_id  first_name ...
        I   2024-01-01 10:00:00     42           Alice      ...
        U   2024-01-15 14:30:00     42           Alicia     ...   <- latest

    The result is a single row with first_name='Alicia' and no Op/_dms_timestamp.

    Given these Bronze rows for customer_id=99:

        Op  _dms_timestamp          customer_id  first_name ...
        I   2024-01-01 10:00:00     99           Bob        ...
        D   2024-02-01 09:00:00     99           Bob        ...   <- deleted

    customer_id=99 does not appear in the result at all.
    """
    # Window: for each primary key, order events newest-first.
    window = Window.partitionBy(pk_col).orderBy(F.col("_dms_timestamp").desc())

    current = (
        df
        # Assign row number 1 to the most recent event per primary key.
        .withColumn("_row_num", F.row_number().over(window))
        # Keep only the latest event.
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
        # Discard rows whose most recent event was a delete.
        .filter(F.col("Op") != "D")
        # Remove CDC metadata, as downstream Silver tables don't need these.
        .drop("Op", "_dms_timestamp")
    )

    return current
