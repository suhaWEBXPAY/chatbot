"""
SQL validation gate — runs BETWEEN sql generation and execution.

Layers on top of db._is_read_only_select (which already blocks non-SELECT and INTO OUTFILE).
This adds: column/table existence, no SELECT *, mandatory LIMIT, and simple cost guards.

Usage:
    ok, safe_sql, problems = validate_sql(sql, schema_map, max_rows=1000)
    if not ok:
        # surface problems, do NOT execute
"""
from __future__ import annotations
import re

# db.py already exposes the hard read-only guard; reuse it, don't reinvent it.
from db import _is_read_only_select  # single source of truth for DML/DDL blocking

MAX_ROWS_DEFAULT = 1000
STATEMENT_TIMEOUT_MS = 15000  # enforce at execution via MySQL MAX_EXECUTION_TIME hint


def _strip(sql: str) -> str:
    return (sql or "").replace("```sql", "").replace("```", "").strip().rstrip(";")


def _referenced_tables(sql: str) -> set[str]:
    # Match `from db.table` or `from table`; keep only the final table name so that
    # schema-qualified names (webxpay_master.tbl_pos_transactions) are not mistaken
    # for an unknown table.
    refs = re.findall(r"\b(?:from|join)\s+`?(?:\w+`?\.`?)?(\w+)`?", sql, flags=re.I)
    return set(refs)


def validate_sql(sql: str, schema_tables: dict[str, set[str]], max_rows: int = MAX_ROWS_DEFAULT):
    """
    schema_tables: {table_name: {col, col, ...}} loaded once from INFORMATION_SCHEMA.
    Returns (ok: bool, safe_sql: str, problems: list[str]).
    """
    problems: list[str] = []
    sql = _strip(sql)

    if not sql:
        return False, sql, ["empty query"]

    if sql.upper().startswith("-- CANNOT_ANSWER"):
        return False, sql, ["model reported the question is unanswerable from the schema"]

    # 1) Hard read-only guard (reuse db.py — do not duplicate the blocklist here)
    if not _is_read_only_select(sql):
        return False, sql, ["not a single read-only SELECT/WITH statement"]

    # 2) No SELECT *  (forces column selection; prevents wide sensitive dumps)
    if re.search(r"select\s+\*", sql, flags=re.I):
        problems.append("SELECT * is not allowed — name explicit columns")

    # 3) Referenced tables must exist in schema
    unknown = _referenced_tables(sql) - set(schema_tables)
    # Exclude CTE names — ANY identifier defined as "<name> AS (" is a CTE (or derived
    # alias), not a base table. This robustly covers WITH a AS (...), b AS (...), ... .
    cte_names = {m.lower() for m in re.findall(r"`?(\w+)`?\s+as\s*\(", sql, flags=re.I)}
    unknown = {u for u in unknown if u.lower() not in cte_names}
    if unknown:
        problems.append(f"unknown table(s): {sorted(unknown)}")

    # 4) Mandatory LIMIT unless it's a pure aggregate with no GROUP BY
    has_group = re.search(r"\bgroup\s+by\b", sql, flags=re.I)
    is_scalar_aggregate = (
        re.search(r"\b(sum|count|avg|min|max)\s*\(", sql, flags=re.I) and not has_group
    )
    if not is_scalar_aggregate and not re.search(r"\blimit\s+\d+", sql, flags=re.I):
        sql = f"{sql}\nLIMIT {max_rows}"          # auto-inject rather than reject
    else:
        m = re.search(r"\blimit\s+(\d+)", sql, flags=re.I)
        if m and int(m.group(1)) > max_rows:
            sql = re.sub(r"\blimit\s+\d+", f"LIMIT {max_rows}", sql, flags=re.I)
            problems.append(f"LIMIT capped to {max_rows}")

    # 5) Cost guard — cross join / cartesian smell (JOIN without ON)
    if re.search(r"\bjoin\b(?!.*\bon\b)", sql, flags=re.I | re.S):
        problems.append("JOIN without ON detected (possible cartesian product)")

    # Column-level existence checks (table.col) — best-effort, warn only
    for tbl, col in re.findall(r"\b(\w+)\.(\w+)\b", sql):
        cols = schema_tables.get(tbl)
        if cols is not None and col.lower() not in {c.lower() for c in cols}:
            # tbl may be an alias; only warn when it's a real table name
            problems.append(f"column {tbl}.{col} not found in schema (may be an alias)")

    # Blocking problems vs warnings: unknown table or SELECT * blocks; the rest warn.
    blocking = any(
        p.startswith("unknown table") or "SELECT *" in p or "cartesian" in p
        for p in problems
    )
    return (not blocking), sql, problems


def with_timeout_hint(sql: str, ms: int = STATEMENT_TIMEOUT_MS) -> str:
    """Add MySQL optimizer hint so a runaway query self-aborts."""
    return re.sub(r"^\s*select\b", f"SELECT /*+ MAX_EXECUTION_TIME({ms}) */", sql,
                  count=1, flags=re.I)
