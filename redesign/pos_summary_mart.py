"""
POS activity summary mart — READ-ONLY-DB version.

Constraint: we cannot create tables or write anything in MySQL. So the pre-computed dedup
lives in a LOCAL SQLite file (summary_mart.sqlite3). Nothing in MySQL changes.

HOW IT WORKS
  - A background job (this script) READS MySQL (allowed) and runs the void-pair dedup for a
    date window, then writes per-store/day/provider valid-sale counts into local SQLite.
  - At query time the chatbot combines the (small, fast) MySQL merchant list with the local
    activity summary in Python — no cross-database JOIN, no heavy GROUP BY at query time.

USAGE
  One-time backfill (all history):   python redesign/pos_summary_mart.py --backfill 2024-01-01
  Nightly incremental refresh:       python redesign/pos_summary_mart.py --refresh
  Status:                            python redesign/pos_summary_mart.py --status

Only READ credentials are needed (same DB_USER/DB_PASSWORD the chatbot uses).
Schedule --refresh nightly via Windows Task Scheduler.
"""
from __future__ import annotations
import os, sys, sqlite3, argparse
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import db_connect  # read-only MySQL connection (chatbot's own)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_PATH = os.path.join(_ROOT, "summary_mart.sqlite3")
DB = os.getenv("DB_NAME", "webxpay_master")

# EXACT dedup logic from the validated build_business_sql valid_pos CTE, aggregated to
# (store, day, provider). Also carries the GMV amount of each unpaired sale so per-merchant
# POS GMV questions (thresholds, rankings) are fast lookups too. Reads MySQL only.
# (Each unpaired group has COUNT(*)=1, so MAX(amount) is that single sale's amount.)
_DEDUP_SQL = f"""
SELECT store_id, d AS activity_date, ipg_provider_id,
       COUNT(*) AS valid_sale_count, SUM(amt) AS valid_sale_amount
FROM (
    SELECT store_id, ipg_provider_id, DATE(transaction_date) AS d, MAX(amount) AS amt
    FROM {DB}.tbl_pos_transactions
    WHERE ipg_provider_id IN (5, 6)
      AND currency = 'LKR'
      AND transaction_date >= %s AND transaction_date < %s
    GROUP BY store_id, ipg_provider_id, DATE(transaction_date),
             invoice_no, auth_code, rrn, terminal_id, terminal_sn
    HAVING COUNT(*) = 1
       AND MAX(LOWER(TRIM(COALESCE(txn_type, '')))) NOT IN
           ('void_sale', 'void-sale', 'void_amex', 'void-amex', '')
) unpaired
GROUP BY store_id, d, ipg_provider_id
"""

# IPG daily GMV per store (approved orders, exact validated FX conversion). Reads MySQL only.
_IPG_GMV_SQL = f"""
SELECT
    o.store_id,
    DATE(p.date_time_transaction) AS d,
    ROUND(SUM(
        CASE
          WHEN o.processing_currency_id = '5' THEN o.total_amount
          WHEN o.exchange_rate IS NOT NULL AND o.exchange_rate NOT LIKE ''
               AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
               THEN o.total_amount * o.exchange_rate
          ELSE o.total_amount * (
            SELECT er.buying_rate FROM {DB}.tbl_exchange_rate er
            WHERE er.currency_id = o.processing_currency_id
              AND er.date <= DATE(p.date_time_transaction)
            ORDER BY er.date DESC LIMIT 1)
        END
    ), 2) AS gmv,
    COUNT(*) AS txn_count
FROM {DB}.tbl_order o
JOIN {DB}.tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND o.processing_currency_id IN ('5','2')
  AND p.date_time_transaction >= %s AND p.date_time_transaction < %s
GROUP BY o.store_id, DATE(p.date_time_transaction)
"""


# Store dimension incl. the RM attribution join chain — snapshotted locally so
# RM-performance questions become ONE local SQLite query (store_dim x daily GMV)
# instead of a live 4-table MySQL join over huge transaction scans (which timed out).
_STORE_DIM_SQL = f"""
SELECT s.store_id,
       MAX(s.doing_business_name)      AS merchant_name,
       MAX(s.registered_name)          AS registered_name,
       MAX(s.is_active)                AS is_active,
       MAX(s.free_trail)               AS free_trail,
       MAX(DATE(s.date_registered))    AS date_registered,
       MAX(wau.name)                   AS rm_name,
       MAX(cc.description)             AS mcc,
       MAX(CASE WHEN ig.store_id IS NOT NULL THEN 1 ELSE 0 END) AS has_ipg,
       MAX(CASE WHEN pm.store_id IS NOT NULL THEN 1 ELSE 0 END) AS has_pos
FROM {DB}.tbl_store s
LEFT JOIN merchant_db.wbx_merchants wm        ON wm.merchant_id = s.store_id
LEFT JOIN merchant_db.wbx_merchant_signups wms ON wms.merchant_id = wm.id
LEFT JOIN merchant_db.wbx_live_rms wlr        ON wlr.id = wms.live_rm_id
LEFT JOIN merchant_db.wbx_admin_users wau     ON wau.id = wlr.admin_user_id
LEFT JOIN {DB}.tbl_category_code cc           ON cc.category_code_id = s.category_code_id
LEFT JOIN (SELECT DISTINCT store_id FROM {DB}.tbl_store_payment_gateway_2
           WHERE is_active = 1) ig ON ig.store_id = s.store_id
LEFT JOIN (SELECT DISTINCT store_id FROM {DB}.tbl_pos_store_bank_mid
           WHERE is_active = 1) pm ON pm.store_id = s.store_id
GROUP BY s.store_id
"""


def _sqlite():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS store_dim (
            store_id INTEGER PRIMARY KEY,
            merchant_name TEXT,
            registered_name TEXT,
            is_active INTEGER,
            free_trail INTEGER,
            date_registered TEXT,
            rm_name TEXT,
            mcc TEXT,
            has_ipg INTEGER DEFAULT 0,
            has_pos INTEGER DEFAULT 0
        )""")
    # add channel-subscription flags when upgrading an older store_dim
    for _col in ("has_ipg", "has_pos"):
        try:
            conn.execute(f"ALTER TABLE store_dim ADD COLUMN {_col} INTEGER DEFAULT 0")
        except Exception:
            pass  # already exists
    # add MCC (merchant category) when upgrading a store_dim built before it
    try:
        conn.execute("ALTER TABLE store_dim ADD COLUMN mcc TEXT")
    except Exception:
        pass  # already exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pos_daily_activity (
            store_id INTEGER NOT NULL,
            activity_date TEXT NOT NULL,
            ipg_provider_id INTEGER NOT NULL,
            valid_sale_count INTEGER NOT NULL,
            valid_sale_amount REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (store_id, activity_date, ipg_provider_id)
        )""")
    # add the amount column if upgrading an older mart built before GMV was tracked
    try:
        conn.execute("ALTER TABLE pos_daily_activity ADD COLUMN valid_sale_amount REAL NOT NULL DEFAULT 0")
    except Exception:
        pass  # already exists
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pda_date ON pos_daily_activity(activity_date)")
    # IPG daily GMV per store (approved orders, exact FX conversion). Cheap to aggregate
    # offline; makes IPG GMV trends/overviews instant instead of a live full-year scan.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ipg_daily_gmv (
            store_id INTEGER NOT NULL,
            activity_date TEXT NOT NULL,
            gmv REAL NOT NULL DEFAULT 0,
            txn_count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (store_id, activity_date)
        )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_idg_date ON ipg_daily_gmv(activity_date)")
    return conn


def _refresh_window(start: date, end: date) -> int:
    """Recompute [start, end): read MySQL dedup, replace that window in local SQLite."""
    mconn = db_connect()
    try:
        cur = mconn.cursor()
        try:
            cur.execute("SET SESSION MAX_EXECUTION_TIME=0")  # backfill windows can run long
        except Exception:
            pass
        cur.execute(_DEDUP_SQL, (start.isoformat(), end.isoformat()))
        pos_rows = cur.fetchall()   # (store_id, date, provider, count, amount)
        cur.execute(_IPG_GMV_SQL, (start.isoformat(), end.isoformat()))
        ipg_rows = cur.fetchall()   # (store_id, date, gmv, txn_count)
        cur.close()
    finally:
        try: mconn.close()
        except Exception: pass

    sconn = _sqlite()
    with sconn:
        sconn.execute("DELETE FROM pos_daily_activity WHERE activity_date >= ? AND activity_date < ?",
                      (start.isoformat(), end.isoformat()))
        sconn.executemany(
            "INSERT OR REPLACE INTO pos_daily_activity "
            "(store_id, activity_date, ipg_provider_id, valid_sale_count, valid_sale_amount) "
            "VALUES (?,?,?,?,?)",
            [(r[0], str(r[1]), r[2], r[3], float(r[4] or 0)) for r in pos_rows],
        )
        sconn.execute("DELETE FROM ipg_daily_gmv WHERE activity_date >= ? AND activity_date < ?",
                      (start.isoformat(), end.isoformat()))
        sconn.executemany(
            "INSERT OR REPLACE INTO ipg_daily_gmv "
            "(store_id, activity_date, gmv, txn_count) VALUES (?,?,?,?)",
            [(r[0], str(r[1]), float(r[2] or 0), r[3]) for r in ipg_rows],
        )
    sconn.close()
    return len(pos_rows) + len(ipg_rows)


def backfill(start_str: str):
    start = datetime.strptime(start_str, "%Y-%m-%d").date().replace(day=1)
    today = date.today()
    total = 0
    cur = start
    while cur <= today:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        n = _refresh_window(cur, min(nxt, today + timedelta(days=1)))
        total += n
        print(f"  {cur:%Y-%m}  -> {n:>7} summary rows")
        cur = nxt
    print(f"[pos_summary_mart] backfill complete: {total} rows in {SQLITE_PATH}")


def refresh_store_dim() -> int:
    """Snapshot store->RM attribution + merchant flags into local SQLite.
    Small query (one row per store); replace the whole table each time."""
    mconn = db_connect()
    try:
        cur = mconn.cursor()
        cur.execute(_STORE_DIM_SQL)
        rows = cur.fetchall()
        cur.close()
    finally:
        try: mconn.close()
        except Exception: pass
    sconn = _sqlite()
    with sconn:
        sconn.execute("DELETE FROM store_dim")
        sconn.executemany(
            "INSERT OR REPLACE INTO store_dim "
            "(store_id, merchant_name, registered_name, is_active, free_trail, "
            " date_registered, rm_name, mcc, has_ipg, has_pos) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(r[0], r[1], r[2], r[3], r[4], str(r[5]) if r[5] else None, r[6], r[7], r[8], r[9])
             for r in rows],
        )
    sconn.close()
    print(f"[pos_summary_mart] store_dim refreshed: {len(rows)} stores")
    return len(rows)


def refresh(days_back: int = 3):
    end = date.today() + timedelta(days=1)
    start = end - timedelta(days=days_back + 1)
    n = _refresh_window(start, end)
    print(f"[pos_summary_mart] refreshed {start}..{end}: {n} rows")
    try:
        refresh_store_dim()
    except Exception as e:
        print(f"[pos_summary_mart] store_dim refresh failed: {e}")


def status():
    if not os.path.exists(SQLITE_PATH):
        print("[pos_summary_mart] not built yet"); return
    conn = _sqlite()
    row = conn.execute("SELECT COUNT(*), MIN(activity_date), MAX(activity_date) FROM pos_daily_activity").fetchone()
    conn.close()
    print(f"[pos_summary_mart] rows={row[0]}  from={row[1]}  to={row[2]}")


# ── read-side helpers used by the chatbot ─────────────────────────────────
def mart_query(sql: str, max_rows: int = 5000):
    """Read-only SQL against the local mart, for the agent engine. Returns
    list[dict] like run_sql, or {"error": ...}. SQLite dialect."""
    import re as _re
    stmt = (sql or "").strip().rstrip(";")
    if not _re.match(r"(?is)^\s*(SELECT|WITH)\b", stmt) or ";" in stmt:
        return {"error": "Only a single read-only SELECT/WITH statement is allowed on the mart."}
    try:
        conn = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(stmt)
            rows = [dict(r) for r in cur.fetchmany(max_rows)]
            return rows
        finally:
            conn.close()
    except Exception as e:
        return {"error": f"mart query failed: {e}"}


def mart_coverage() -> dict:
    """Date coverage + store_dim size, for the agent prompt. {} when not built."""
    try:
        conn = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True, timeout=5)
        try:
            out = {}
            r = conn.execute("SELECT MIN(activity_date), MAX(activity_date) FROM pos_daily_activity").fetchone()
            out["pos_from"], out["pos_to"] = r[0], r[1]
            r = conn.execute("SELECT MIN(activity_date), MAX(activity_date) FROM ipg_daily_gmv").fetchone()
            out["ipg_from"], out["ipg_to"] = r[0], r[1]
            try:
                out["stores"] = conn.execute("SELECT COUNT(*) FROM store_dim").fetchone()[0]
            except Exception:
                out["stores"] = 0
            return out if out.get("pos_to") or out.get("ipg_to") else {}
        finally:
            conn.close()
    except Exception:
        return {}


_ready_cache = None

def summary_ready() -> bool:
    global _ready_cache
    if _ready_cache is not None:
        return _ready_cache
    try:
        if not os.path.exists(SQLITE_PATH):
            _ready_cache = False
        else:
            conn = sqlite3.connect(SQLITE_PATH)
            n = conn.execute("SELECT COUNT(*) FROM pos_daily_activity").fetchone()[0]
            conn.close()
            _ready_cache = n > 0
    except Exception:
        _ready_cache = False
    return _ready_cache


def transacted_store_ids(ds: str, de: str) -> set[int]:
    """Store IDs with >=1 valid POS sale in [ds, de) — fast local SQLite lookup."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        cur = conn.execute(
            "SELECT DISTINCT store_id FROM pos_daily_activity "
            "WHERE activity_date >= ? AND activity_date < ? AND valid_sale_count > 0",
            (ds, de))
        return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def ipg_transacted_store_ids(ds: str, de: str) -> set[int]:
    """Store IDs with >=1 approved IPG transaction in [ds, de) — fast local lookup."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        cur = conn.execute(
            "SELECT DISTINCT store_id FROM ipg_daily_gmv "
            "WHERE activity_date >= ? AND activity_date < ? AND txn_count > 0",
            (ds, de))
        return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def ipg_ready() -> bool:
    try:
        if not os.path.exists(SQLITE_PATH):
            return False
        conn = sqlite3.connect(SQLITE_PATH)
        n = conn.execute("SELECT COUNT(*) FROM ipg_daily_gmv").fetchone()[0]
        conn.close()
        return n > 0
    except Exception:
        return False


def ipg_monthly_gmv(ds: str, de: str) -> dict:
    """{ 'YYYY-MM': total IPG GMV } for [ds, de) — instant local lookup."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        cur = conn.execute(
            "SELECT substr(activity_date,1,7) AS ym, SUM(gmv), SUM(txn_count) "
            "FROM ipg_daily_gmv WHERE activity_date >= ? AND activity_date < ? "
            "GROUP BY ym ORDER BY ym", (ds, de))
        return {r[0]: (float(r[1] or 0), int(r[2] or 0)) for r in cur.fetchall()}
    finally:
        conn.close()


def pos_monthly_gmv(ds: str, de: str) -> dict:
    """{ 'YYYY-MM': total valid POS GMV } for [ds, de) — instant local lookup."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        cur = conn.execute(
            "SELECT substr(activity_date,1,7) AS ym, SUM(valid_sale_amount) "
            "FROM pos_daily_activity WHERE activity_date >= ? AND activity_date < ? "
            "GROUP BY ym ORDER BY ym", (ds, de))
        return {r[0]: float(r[1] or 0) for r in cur.fetchall()}
    finally:
        conn.close()


def store_gmv(ds: str, de: str) -> dict[int, float]:
    """{store_id: total valid POS GMV} for [ds, de) — fast local SQLite lookup."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        cur = conn.execute(
            "SELECT store_id, SUM(valid_sale_amount) FROM pos_daily_activity "
            "WHERE activity_date >= ? AND activity_date < ? GROUP BY store_id",
            (ds, de))
        return {r[0]: float(r[1] or 0) for r in cur.fetchall()}
    finally:
        conn.close()


def _last_month_bounds():
    first_this = date.today().replace(day=1)
    last_prev_end = first_this
    last_prev_start = (first_this - timedelta(days=1)).replace(day=1)
    return last_prev_start.isoformat(), last_prev_end.isoformat()


def _parse_threshold(question: str):
    """Return (op, value) from 'less than 350k', 'more than 1m', 'below 500000', or None.
    Ignores PERCENT/change questions ('dropped more than 10%', 'grew 15 percent') — those
    are comparisons, not GMV thresholds."""
    import re
    ql = question.lower()
    # A percentage or change/comparison question is NOT a GMV-amount threshold.
    if any(w in ql for w in ("%", "percent", "drop", "dropped", "decline", "declined",
                             "fell", "fall", "increase", "increased", "grew", "growth",
                             "compared", " vs ", "versus", "change")):
        return None
    m = re.search(
        r"(less than|under|below|lower than|<|more than|over|above|greater than|higher than|>|at least|at most)\s*"
        r"(?:rs\.?|lkr)?\s*([\d,]+(?:\.\d+)?)\s*(k|m|mn|million|thousand)?",
        ql)
    if not m:
        return None
    direction, num, suf = m.group(1), m.group(2).replace(",", ""), (m.group(3) or "")
    val = float(num)
    if suf in ("k", "thousand"):    val *= 1_000
    elif suf in ("m", "mn", "million"): val *= 1_000_000
    lt = direction in ("less than", "under", "below", "lower than", "<", "at most")
    return ("<", val) if lt else (">", val)


def handle_pos_merchant_gmv(question: str, ds: str | None, de: str | None, sql_executor) -> dict:
    """Per-merchant POS GMV in a period, read from the local mart (fast). Applies a
    'less/more than X' threshold if present; otherwise returns all active POS merchants
    with their GMV (lowest first)."""
    if not ds or not de:
        ds, de = _last_month_bounds()

    merchants = sql_executor(_MERCHANT_LIST_SQL)
    if isinstance(merchants, dict) and "error" in merchants:
        return {"question": question, "sql": _MERCHANT_LIST_SQL, "raw_result": merchants,
                "answer": f"**Database error:** {merchants['error']}", "insights": "",
                "response_type": "error", "engine": "new"}

    gmv = store_gmv(ds, de)
    rows = [{"merchant_name": m.get("merchant_name"), "store_id": m.get("store_id"),
             "pos_gmv_lkr": round(gmv.get(m.get("store_id"), 0.0), 2)} for m in merchants]

    thr = _parse_threshold(question)
    if thr:
        op, val = thr
        rows = [r for r in rows if (r["pos_gmv_lkr"] < val if op == "<" else r["pos_gmv_lkr"] > val)]
        cond = f"{'less than' if op == '<' else 'more than'} LKR {val:,.0f}"
    else:
        cond = "any amount"
    rows.sort(key=lambda r: r["pos_gmv_lkr"])

    answer = (f"**{len(rows)} POS merchants had {cond}** in POS GMV between {ds} and {de}.\n\n"
              "POS GMV = valid (non-voided) POS sales, from the pre-computed activity summary "
              "(matches the validated dedup logic). Sorted lowest GMV first.")
    return {"question": question,
            "sql": {"merchant_list_mysql": _MERCHANT_LIST_SQL.strip(),
                    "gmv": f"local summary_mart.sqlite3 -> pos_daily_activity, {ds}..{de}"},
            "raw_result": rows, "answer": answer, "insights": answer,
            "response_type": "detail_rows", "engine": "new"}


# Merchant list is small + read-only (MySQL). Non-transacting = list minus transacted set.
_MERCHANT_LIST_SQL = f"""
SELECT s.store_id, s.doing_business_name AS merchant_name
FROM {DB}.tbl_store s
INNER JOIN (
    SELECT DISTINCT store_id FROM {DB}.tbl_pos_store_bank_mid WHERE is_active = 1
) m ON m.store_id = s.store_id
WHERE s.free_trail = 0 AND s.is_active = 1
ORDER BY s.doing_business_name
"""

def handle_pos_non_transacting(question: str, ds: str | None, de: str | None, sql_executor) -> dict:
    """FULL handler: MySQL merchant list (fast) minus locally-summarised transacting stores."""
    y = date.today().year
    ds = ds or f"{y}-01-01"
    de = de or f"{y + 1}-01-01"

    merchants = sql_executor(_MERCHANT_LIST_SQL)
    if isinstance(merchants, dict) and "error" in merchants:
        return {"question": question, "sql": _MERCHANT_LIST_SQL, "raw_result": merchants,
                "answer": f"**Database error:** {merchants['error']}", "insights": "",
                "response_type": "error", "engine": "new"}

    transacted = transacted_store_ids(ds, de)
    rows = [m for m in merchants if m.get("store_id") not in transacted]
    active_transacted = len(merchants) - len(rows)   # only among active POS merchants

    answer = (f"**{len(rows)} POS merchants did not transact** between {ds} and {de} "
              f"(out of {len(merchants)} active POS merchants; {active_transacted} of them transacted).\n\n"
              "A merchant counts as 'transacting' if it had at least one valid (non-voided) POS "
              "sale in that period. Figures use the pre-computed activity summary, which matches "
              "the validated dedup logic.")
    return {
        "question": question,
        "sql": {"merchant_list_mysql": _MERCHANT_LIST_SQL.strip(),
                "activity": "local summary_mart.sqlite3 -> pos_daily_activity"},
        "raw_result": rows,
        "answer": answer,
        "insights": answer,
        "response_type": "detail_rows",
        "engine": "new",
    }


def handle_non_transacting(question: str, ds: str | None, de: str | None,
                           sql_executor=None, channel: str = "any"):
    """Channel-aware non-transacting merchants, entirely from the local mart.
    channel: 'pos' | 'ipg' | 'any' ('any' = no valid POS sale AND no approved IPG txn).
    Returns None when the requested window starts before mart coverage, so the
    caller can fall through to the live pipeline."""
    y = date.today().year
    ds = ds or f"{y}-01-01"
    de = de or f"{y + 1}-01-01"

    cov = mart_coverage()
    cov_from = max(cov.get("pos_from") or "9999", cov.get("ipg_from") or "9999")
    if not cov or str(ds) < cov_from:
        return None

    sub_filter = {"pos": "AND has_pos = 1", "ipg": "AND has_ipg = 1"}.get(channel, "")
    merchants = mart_query(
        "SELECT store_id, merchant_name, registered_name, rm_name, has_ipg, has_pos "
        f"FROM store_dim WHERE is_active = 1 AND free_trail = 0 {sub_filter} "
        "ORDER BY merchant_name")
    if not isinstance(merchants, list) or not merchants:
        return None

    transacted: set[int] = set()
    if channel in ("pos", "any"):
        transacted |= transacted_store_ids(ds, de)
    if channel in ("ipg", "any"):
        transacted |= ipg_transacted_store_ids(ds, de)

    rows = [m for m in merchants if m.get("store_id") not in transacted]
    n_trans = len(merchants) - len(rows)

    chan_desc = {
        "pos": ("active POS merchants", "at least one valid (non-voided) POS sale"),
        "ipg": ("active IPG merchants", "at least one approved IPG transaction"),
    }.get(channel, ("active merchants",
                    "at least one valid POS sale or approved IPG transaction"))

    # Period honesty: never present a future end date as covered by data.
    cov_to = max(cov.get("pos_to") or "", cov.get("ipg_to") or "")
    de_shown = min(str(de), cov_to) if cov_to else str(de)

    answer = (f"**{len(rows)} {chan_desc[0]} did not transact** between {ds} and {de_shown} "
              f"(out of {len(merchants)}; {n_trans} of them transacted).\n\n"
              f"A merchant counts as 'transacting' if it had {chan_desc[1]} in that period. "
              "Figures come from the pre-computed activity summary (validated dedup/FX logic).")
    return {
        "question": question,
        "sql": {"source": f"local summary_mart.sqlite3 (store_dim + activity tables), channel={channel}",
                "period": f"{ds}..{de}"},
        "raw_result": rows,
        "answer": answer,
        "insights": answer,
        "response_type": "detail_rows",
        "engine": "new",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", metavar="YYYY-MM-DD")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--days-back", type=int, default=3)
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--store-dim", action="store_true")
    a = ap.parse_args()
    if a.backfill:   backfill(a.backfill)
    elif a.refresh:  refresh(a.days_back)
    elif a.store_dim: refresh_store_dim()
    elif a.status:   status()
    else:            ap.print_help()
