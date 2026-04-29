import mysql.connector
from mysql.connector import pooling, Error
import re

# --- MASTER DATABASE CONFIG ---
_POOL_CONFIG = {
    "host": "prod-db-read.webxpay.com",
    "user": "readuser01",
    "password": "hV51KziWB3hSlc8Gu",
    "database": "webxpay_master",
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


def run_sql(query):
    try:
        query = (query or "").replace("```sql", "").replace("```", "").strip()
        print("\n[run_sql] Executing SQL (first 500 chars):\n", query[:500], "\n")

        if not query.lstrip().upper().startswith("SELECT"):
            return {"error": "Only SELECT queries are allowed."}

        conn = db_connect()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data

    except Exception as e:
        print("[run_sql] SQL ERROR:", str(e))
        return {"error": str(e)}


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
