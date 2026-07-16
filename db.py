import os
import mysql.connector
from mysql.connector import pooling, Error
from dotenv import load_dotenv
import re

load_dotenv()

# --- MASTER DATABASE CONFIG ---
_POOL_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 60,
}

try:
    _pool = pooling.MySQLConnectionPool(
        pool_name="webxpay_pool",
        pool_size=8,
        pool_reset_session=True,
        **_POOL_CONFIG,
    )
    print("Connection pool created (size=8)")
except Error as e:
    print(f"Pool creation failed: {e}")
    _pool = None


def db_connect():
    if _pool:
        return _pool.get_connection()
    return mysql.connector.connect(**_POOL_CONFIG)


def _sql_without_comments_or_literals(query):
    """Return SQL with comments and quoted strings removed for safety checks."""
    query = re.sub(r"/\*.*?\*/", " ", query, flags=re.S)
    query = re.sub(r"(--|#)[^\r\n]*", " ", query)
    query = re.sub(r"'(?:''|\\'|[^'])*'", "''", query)
    query = re.sub(r'"(?:""|\\\"|[^"])*"', '""', query)
    return query


def _is_read_only_select(query):
    cleaned = _sql_without_comments_or_literals(query).strip()
    statements = [s.strip() for s in cleaned.split(";") if s.strip()]

    if len(statements) != 1:
        return False

    statement = statements[0]
    if not re.match(r"^(SELECT|WITH)\b", statement, flags=re.I):
        return False

    forbidden = (
        r"\b(INSERT|UPDATE|DELETE|REPLACE|MERGE|DROP|ALTER|CREATE|TRUNCATE|"
        r"RENAME|GRANT|REVOKE|CALL|EXEC|LOAD|SET|LOCK|UNLOCK)\b"
    )
    if re.search(forbidden, statement, flags=re.I):
        return False

    if re.search(r"\bINTO\s+(OUTFILE|DUMPFILE)\b", statement, flags=re.I):
        return False

    return True


def run_sql(query):
    query = (query or "").replace("```sql", "").replace("```", "").strip()
    print("\n[run_sql] Executing SQL (first 500 chars):\n", query[:500], "\n")

    if not _is_read_only_select(query):
        return {"error": "Only read-only SELECT queries are allowed."}

    conn = None
    cursor = None
    try:
        conn = db_connect()
        # A pooled connection can go stale between requests; revive it before use.
        try:
            conn.ping(reconnect=True, attempts=3, delay=1)
        except Exception:
            pass
        cursor = conn.cursor(dictionary=True)
        # Hard server-side cap so a truly runaway query self-aborts instead of hanging the
        # request forever. Generous by default (heavy analytics can legitimately take a
        # while); override with SQL_MAX_EXECUTION_MS if needed.
        _timeout_ms = int(os.getenv("SQL_MAX_EXECUTION_MS", "180000"))  # 3 minutes
        try:
            cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={_timeout_ms}")
        except Exception:
            pass
        cursor.execute(query)
        data = cursor.fetchall()
        return data

    except Exception as e:
        msg = str(e)
        print("[run_sql] SQL ERROR:", msg)
        # Friendly message when a query hits the execution-time cap.
        if "max_execution_time" in msg.lower() or "3024" in msg or "statement execution time" in msg.lower():
            return {"error": ("This query needs to scan a lot of data and took too long to "
                              "finish. Try narrowing the date range (e.g. a single month), or "
                              "ask for a summary/total instead of every row.")}
        return {"error": msg}

    finally:
        # ALWAYS release the connection back to the pool, even on error/timeout —
        # otherwise leaked connections exhaust the pool ("MySQL Connection not available").
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()   # for a pooled connection, this returns it to the pool
            except Exception:
                pass


def get_columns_for_table(table_name):
    try:
        conn = db_connect()
        cursor = conn.cursor()
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return columns
    except Exception:
        return []
