"""
Live schema loader — reads the REAL tables/columns from the database instead of the
hand-maintained schema.txt (which has drifted: first it was missing tbl_pos_store_bank_mid,
then missing the cost_rate column).

- get_schema_tables()  -> {table_name: {col, col, ...}}   for the validator
- get_schema_text()    -> schema.txt-style string           for the LLM prompts

Both are cached in-process (loaded once). Call refresh() after a DB migration.
If the DB is unreachable, both fall back to parsing schema.txt so nothing breaks offline.
"""
from __future__ import annotations
import os, re

# db.py lives one directory up; import lazily so this module also works standalone.
try:
    from db import db_connect
except Exception:  # pragma: no cover
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from db import db_connect

_SCHEMA_TXT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "schema.txt")

# Cache
_tables_cache: dict[str, set[str]] | None = None
_text_cache: str | None = None
_source: str = "uninitialized"   # "database" or "schema.txt" — for observability


# ─────────────────────────────────────────────────────────────────────────
# Fallback: parse the existing schema.txt (same format the old code used)
# ─────────────────────────────────────────────────────────────────────────
def _parse_schema_txt() -> dict[str, set[str]]:
    tables: dict[str, set[str]] = {}
    try:
        with open(_SCHEMA_TXT, "r", encoding="utf-8") as f:
            for line in f:
                m = re.match(r"\s*-\s*(\w+)\((.*)\)", line)
                if m:
                    tables[m.group(1)] = {c.strip() for c in m.group(2).split(",") if c.strip()}
    except Exception:
        pass
    return tables


# ─────────────────────────────────────────────────────────────────────────
# Primary: read live from INFORMATION_SCHEMA
# ─────────────────────────────────────────────────────────────────────────
def _load_from_db() -> dict[str, set[str]]:
    """One query returns every column of every table in the current database."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        # DATABASE() scopes to the connection's own schema (from DB_NAME) — no hardcoding.
        # Load the main DB plus merchant_db (RM / signup tables live there).
        _extra = [s.strip() for s in os.getenv("EXTRA_DB_SCHEMAS", "merchant_db").split(",") if s.strip()]
        _schemas = "DATABASE()" + ("".join(f", '{s}'" for s in _extra))
        cur.execute(
            f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA IN ({_schemas})
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
            """
        )
        tables: dict[str, set[str]] = {}
        for table_name, column_name in cur.fetchall():
            tables.setdefault(table_name, set()).add(column_name)
        cur.close()
        return tables
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────
def get_schema_tables() -> dict[str, set[str]]:
    """{table: {columns}} — live from DB, cached, with schema.txt fallback."""
    global _tables_cache, _source
    if _tables_cache is not None:
        return _tables_cache
    try:
        tables = _load_from_db()
        if tables:                      # got a real answer from the DB
            # Merge in any tables declared in schema.txt but not yet in the live DB
            # (e.g. pos_daily_activity before the mart is built). Live columns win.
            for t, cols in _parse_schema_txt().items():
                tables.setdefault(t, cols)
            _tables_cache, _source = tables, "database"
            return _tables_cache
    except Exception as e:
        print(f"[schema_loader] live load failed ({e}); falling back to schema.txt")
    _tables_cache, _source = _parse_schema_txt(), "schema.txt"
    return _tables_cache


def get_schema_text() -> str:
    """
    schema.txt-style text for LLM prompts. Built from the LIVE schema when available
    so the planner/SQL prompts always see the real columns; else the raw file.
    """
    global _text_cache
    if _text_cache is not None:
        return _text_cache
    tables = get_schema_tables()
    if _source == "database" and tables:
        lines = ["Tables:"]
        for t in sorted(tables):
            # keep a stable, readable order; DB set() has no order so just sort columns
            cols = ", ".join(sorted(tables[t]))
            lines.append(f"- {t}({cols})")
        _text_cache = "\n".join(lines)
    else:
        try:
            with open(_SCHEMA_TXT, "r", encoding="utf-8") as f:
                _text_cache = f.read().strip()
        except Exception:
            _text_cache = ""
    return _text_cache


def source() -> str:
    """'database' or 'schema.txt' — which source the current cache came from."""
    get_schema_tables()
    return _source


def refresh() -> None:
    """Clear caches so the next call reloads (use after a DB migration)."""
    global _tables_cache, _text_cache, _source
    _tables_cache = _text_cache = None
    _source = "uninitialized"


if __name__ == "__main__":
    tabs = get_schema_tables()
    print(f"source: {source()}")
    print(f"tables: {len(tabs)}")
    for t in sorted(tabs):
        print(f"  {t}: {len(tabs[t])} cols")
