"""
Glue Job lifecycle helpers.

job.init() and job.commit() serve two purposes in AWS Glue:
  1. Register the job run with the Glue service (for monitoring in the console).
  2. Save/load the job bookmark, the cursor that tells Glue which S3 files
     have already been processed so incremental jobs skip them.

Both require AWS credentials and a reachable Glue service endpoint. When
running locally in Docker against file:// paths, neither is available:
the EC2 metadata service is not present, and there is no Glue endpoint.

Our jobs do full CDC reconciliation on every run (no incremental bookmarking),
so bookmarks are intentionally unused. The try/except here lets the Spark
transformations run without change regardless of environment.

In AWS (deployed via make deploy), job.init() succeeds normally because the
container has an IAM role with Glue permissions and the endpoint is reachable.
"""

from __future__ import annotations

import sys


def init_job(job: object, job_name: str, args: dict) -> None:
    """
    Call job.init(), ignoring failures in local Docker environments where
    the Glue service endpoint and AWS credentials are not available.
    """
    try:
        job.init(job_name, args)
    except Exception as exc:
        print(
            f"[job_utils] job.init() skipped (local mode — no Glue endpoint): {exc}",
            file=sys.stderr,
        )


def commit_job(job: object) -> None:
    """
    Call job.commit(), ignoring failures in local Docker environments.
    """
    try:
        job.commit()
    except Exception as exc:
        print(
            f"[job_utils] job.commit() skipped (local mode — no Glue endpoint): {exc}",
            file=sys.stderr,
        )
