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

    # Add a boolean column for each rule: True = passed, False = failed.
    annotated = df
    check_cols = []
    for rule_name, expr in rules.items():
        col_name = f"_check_{rule_name}"
        annotated = annotated.withColumn(col_name, F.expr(expr))
        check_cols.append((col_name, rule_name))

    # A row is valid only if every check column is True.
    all_pass_expr = " AND ".join(col for col, _ in check_cols)

    valid_df = annotated.filter(all_pass_expr).drop(*[col for col, _ in check_cols])

    invalid_df = annotated.filter(f"NOT ({all_pass_expr})")

    # Count invalid rows. If there are none, skip the quarantine write.
    invalid_count = invalid_df.count()

    if invalid_count > 0:
        # Build a human-readable _validation_errors column that lists
        # every rule the row failed, separated by commas.
        error_parts = [
            F.when(~F.col(col), F.lit(rule_name)).otherwise(F.lit(None))
            for col, rule_name in check_cols
        ]
        quarantine_df = (
            invalid_df
            .withColumn(
                "_validation_errors",
                F.concat_ws(", ", *error_parts),
            )
            .drop(*[col for col, _ in check_cols])
        )

        quarantine_df.write.mode("append").parquet(f"{quarantine_path}/{table_name}")

        print(  # noqa: T201
            f"[validation] {table_name}: {invalid_count} rows quarantined "
            f"to {quarantine_path}/{table_name}"
        )

    return valid_df
