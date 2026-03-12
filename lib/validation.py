"""
Data quality validation for Silver jobs.

Every Glue job runs its output DataFrame through validate() before writing
to Silver. Rows that fail any rule are written to the Quarantine bucket
instead of Silver, with an extra column (_validation_errors) that names
every rule the row failed.

This means Silver always contains clean, trustworthy data. Quarantine
captures everything that didn't make the cut so nothing is silently lost.

Rules are expressed as Spark SQL boolean expressions — the same syntax
you'd use in a DataFrame.filter() or WHERE clause. A rule passes when
the expression evaluates to True.

Example rules:
    {
        "customer_id_not_null": "customer_id IS NOT NULL",
        "email_not_null":       "email IS NOT NULL",
        "country_not_null":     "country IS NOT NULL",
    }

Any row where customer_id IS NULL, email IS NULL, OR country IS NULL
is quarantined with a _validation_errors column like:
    "customer_id_not_null, email_not_null"
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate(
    df: DataFrame,
    rules: dict[str, str],
    quarantine_path: str,
    table_name: str,
) -> DataFrame:
    """
    Split a DataFrame into valid and invalid rows.

    Valid rows are returned. Invalid rows are written to quarantine and
    NOT included in the return value.

    Parameters
    ----------
    df:
        The DataFrame to validate (after CDC reconciliation, before writing Silver).
    rules:
        Dict mapping rule name -> Spark SQL boolean expression.
        A row is valid if ALL expressions evaluate to True.
    quarantine_path:
        Root quarantine path (from Paths.quarantine_root). Invalid rows are
        written to quarantine_path/table_name/.
    table_name:
        Used as the subdirectory under quarantine_path and in log messages.

    Returns
    -------
    DataFrame containing only rows that passed all validation rules.
    """
    if not rules:
        return df

    check_col_names = [f"_check_{name}" for name in rules]

    # Add all check columns in a single select() rather than chained withColumn()
    # calls. Chained withColumn() in a loop builds a longer and longer query plan
    # on every iteration, which causes quadratic plan-analysis time at scale.
    # A single select() builds the same logical plan in one step.
    annotated = df.select(
        "*",
        *[F.expr(expr).alias(f"_check_{name}") for name, expr in rules.items()],
    )

    # Cache annotated so that Spark does not re-execute the input DAG for both
    # the valid write path and the quarantine write path. Without caching,
    # each downstream action (quarantine_df.write and the caller's Silver write)
    # independently triggers the full plan — Bronze read + CDC window function +
    # check column expressions — from scratch.
    #
    # We do NOT call unpersist() here. validate() returns valid_df as a lazy
    # DataFrame; the caller triggers the Silver write after this function returns.
    # If we called unpersist() before returning, annotated would be gone by the
    # time the caller writes, and Spark would re-execute the full DAG anyway —
    # defeating the cache entirely. Glue jobs are short-lived; Spark releases all
    # cached data automatically when the job completes.
    annotated.cache()

    all_pass_expr = " AND ".join(check_col_names)

    valid_df = annotated.filter(all_pass_expr).drop(*check_col_names)
    invalid_df = annotated.filter(f"NOT ({all_pass_expr})")

    # Build a human-readable _validation_errors column that names every rule
    # the row failed, then write invalid rows to quarantine.
    # The write() call is the only action we trigger on this path — no
    # separate count() needed to guard it, because writing an empty DataFrame
    # is a no-op for Parquet (Spark writes zero files and exits cleanly).
    # This write also materialises the annotated cache so the Silver write
    # (triggered by the caller) reuses it instead of re-reading Bronze.
    error_parts = [
        F.when(~F.col(col), F.lit(name)).otherwise(F.lit(None))
        for col, name in zip(check_col_names, rules)
    ]
    (
        invalid_df
        .withColumn("_validation_errors", F.concat_ws(", ", *error_parts))
        .drop(*check_col_names)
        .write.mode("append")
        .parquet(f"{quarantine_path}/{table_name}")
    )

    return valid_df
