#!/usr/bin/env python3
"""
athena_apply.py

Apply Athena SQL files (DDL + MSCK REPAIR) from a folder, in order.

Typical use:
  python athena_apply.py --sql-dir /opt/sql/athena

Env vars expected:
  AWS_REGION            (optional if your container has it)
  ATHENA_WORKGROUP      (default: primary)
  ATHENA_DATABASE       (default: crypto_lakehouse)
  ATHENA_OUTPUT_S3      (required) e.g. s3://my-bucket/athena-results/
  ATHENA_SQL_DIR        (optional default if --sql-dir not given)

  ATHENA_QUERY_TIMEOUT_SEC (default: 900)
  ATHENA_FILES          (optional) comma-separated list of SQL filenames to apply
                        e.g. "99_repair_partitions.sql"
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

import boto3


def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise ValueError(f"Missing env var {name}")
    return v


def list_sql_files(sql_dir: Path) -> List[Path]:
    if not sql_dir.exists() or not sql_dir.is_dir():
        raise ValueError(f"SQL dir does not exist or is not a directory: {sql_dir}")

    files = sorted([p for p in sql_dir.glob("*.sql") if p.is_file()])
    if not files:
        raise ValueError(f"No .sql files found in: {sql_dir}")
    return files


def read_sql_file(path: Path) -> str:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"Empty SQL file: {path}")
    return text


def split_statements(sql_text: str) -> List[str]:
    """
    Very small splitter:
    - splits on ';'
    - removes empty statements
    - ignores lines starting with '--'
    Works well for simple Athena DDL + MSCK REPAIR scripts.
    """
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            continue
        lines.append(line)
    cleaned = "\n".join(lines)

    stmts = []
    for chunk in cleaned.split(";"):
        s = chunk.strip()
        if s:
            stmts.append(s)
    return stmts


def _parse_athena_files_env(sql_dir: Path) -> Optional[List[Path]]:
    """
    If ATHENA_FILES is set (comma-separated filenames), return the ordered list of Path objects.
    Otherwise return None.
    """
    files_env = os.environ.get("ATHENA_FILES", "").strip()
    if not files_env:
        return None

    wanted = [f.strip() for f in files_env.split(",") if f.strip()]
    if not wanted:
        return None

    missing = []
    paths: List[Path] = []
    for name in wanted:
        p = sql_dir / name
        if not p.exists() or not p.is_file():
            missing.append(name)
        else:
            paths.append(p)

    if missing:
        raise ValueError(f"ATHENA_FILES references missing file(s) in {sql_dir}: {missing}")

    return paths


def start_query(
    athena,
    statement: str,
    database: str,
    output_s3: str,
    workgroup: str,
) -> str:
    resp = athena.start_query_execution(
        QueryString=statement,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_s3},
        WorkGroup=workgroup,
    )
    return resp["QueryExecutionId"]


def wait_query(
    athena,
    qid: str,
    timeout_sec: int = 900,
    poll_sec: float = 1.5,
) -> Tuple[str, Optional[str]]:
    """
    Returns: (state, reason)
    state in: SUCCEEDED | FAILED | CANCELLED
    """
    deadline = time.time() + timeout_sec
    last_state = None

    while time.time() < deadline:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        status = resp["QueryExecution"]["Status"]
        state = status["State"]
        reason = status.get("StateChangeReason")

        if state != last_state:
            last_state = state

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state, reason

        time.sleep(poll_sec)

    # Timeout: attempt to stop the query
    try:
        athena.stop_query_execution(QueryExecutionId=qid)
    except Exception:
        pass
    return "CANCELLED", f"Timeout after {timeout_sec}s (query stopped)"


def apply_file(
    athena,
    sql_path: Path,
    database: str,
    output_s3: str,
    workgroup: str,
    timeout_sec: int,
) -> None:
    sql_text = read_sql_file(sql_path)
    statements = split_statements(sql_text)

    print(f"\n==> Applying file: {sql_path.name} ({len(statements)} statement(s))")

    for i, stmt in enumerate(statements, start=1):
        preview = stmt.replace("\n", " ")
        preview = (preview[:160] + "...") if len(preview) > 160 else preview
        print(f"  -> [{i}/{len(statements)}] {preview}")

        qid = start_query(
            athena=athena,
            statement=stmt,
            database=database,
            output_s3=output_s3,
            workgroup=workgroup,
        )

        state, reason = wait_query(athena, qid, timeout_sec=timeout_sec)

        if state != "SUCCEEDED":
            raise RuntimeError(
                f"Athena query failed.\n"
                f"File: {sql_path.name}\n"
                f"Statement #{i}: {stmt}\n"
                f"QueryExecutionId: {qid}\n"
                f"State: {state}\n"
                f"Reason: {reason}\n"
            )

    print(f"✅ Done: {sql_path.name}")


def main():
    parser = argparse.ArgumentParser(description="Apply Athena SQL files from a folder.")
    parser.add_argument(
        "--sql-dir",
        default=os.environ.get("ATHENA_SQL_DIR", ""),
        help="Directory containing .sql files (default: ATHENA_SQL_DIR env var).",
    )
    parser.add_argument(
        "--only",
        default="",
        help="Apply only SQL files containing this substring (e.g. 'repair' or 'create_gold').",
    )
    args = parser.parse_args()

    if not args.sql_dir:
        raise ValueError("Provide --sql-dir or set ATHENA_SQL_DIR")

    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    workgroup = os.environ.get("ATHENA_WORKGROUP", "primary")
    database = os.environ.get("ATHENA_DATABASE", "crypto_lakehouse")
    output_s3 = _require_env("ATHENA_OUTPUT_S3")
    timeout_sec = int(os.environ.get("ATHENA_QUERY_TIMEOUT_SEC", "900"))

    sql_dir = Path(args.sql_dir)

    # Default: all *.sql sorted
    files = list_sql_files(sql_dir)

    # NEW: if ATHENA_FILES is set, override to only those files (in that order)
    override_files = _parse_athena_files_env(sql_dir)
    if override_files is not None:
        files = override_files

    # Keep existing CLI filter (applied after ATHENA_FILES override)
    if args.only:
        files = [p for p in files if args.only in p.name]
        if not files:
            raise ValueError(f"No files matched --only='{args.only}' in {sql_dir}")

    print("Athena apply configuration:")
    print(f"  region     = {region}")
    print(f"  workgroup  = {workgroup}")
    print(f"  database   = {database}")
    print(f"  output_s3  = {output_s3}")
    print(f"  sql_dir    = {sql_dir.resolve()}")
    print(f"  files      = {[p.name for p in files]}")

    athena = boto3.client("athena", region_name=region)

    for p in files:
        apply_file(
            athena=athena,
            sql_path=p,
            database=database,
            output_s3=output_s3,
            workgroup=workgroup,
            timeout_sec=timeout_sec,
        )

    print("\n✅ All Athena SQL applied successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        sys.exit(1)
