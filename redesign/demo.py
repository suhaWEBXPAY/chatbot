"""
RUNNABLE DEMO of the new flow — one real example, end to end.

Run it:   python redesign/demo.py "what was the IPG revenue for March 2025?"

It shows each stage in the new pipeline:
  1. ROUTER  — AI decides: database / web / greeting?
  2. PLANNER — AI understands the question and picks a TOOL + fills in exact dates
  3. SQL     — the chosen tool builds the query (reusing YOUR existing builders)
  4. VALIDATE— the safety checker approves or fixes the query

By default it STOPS before running the query against your real database (safe / dry-run).
Add --execute to actually run it (needs your DB reachable).
"""
import sys, os, json, re

# make sibling project modules importable when run from anywhere
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # project root (gpt_helpers, db)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))                    # this folder (validator, schema_loader, answer)

from datetime import date
import gpt_helpers as G           # reuse your existing LLM client + SQL builders
from validator import validate_sql, with_timeout_hint
from schema_loader import get_schema_tables, source as schema_source


# ─────────────────────────────────────────────────────────────────────────
# tiny LLM helper (reuses your Gemini client from gpt_helpers)
# ─────────────────────────────────────────────────────────────────────────
def _with_retry(fn, attempts: int = 4, base_delay: float = 1.0):
    """Retry an LLM call on transient errors (Gemini 503 'overloaded', rate limits)."""
    import time
    last = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            last = e
            msg = str(e).lower()
            transient = any(t in msg for t in ("503", "overloaded", "rate", "429", "timeout", "unavailable"))
            if not transient or i == attempts - 1:
                raise
            time.sleep(base_delay * (2 ** i))   # 1s, 2s, 4s backoff
    raise last


def _safe_json(raw: str):
    """Parse LLM output into a dict, tolerating fences, prose, and minor malformation.
    Returns None if it truly can't be parsed."""
    if not raw:
        return None
    raw = raw.strip()
    raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw).strip()
    m = re.search(r"\{.*\}", raw, re.DOTALL)          # outermost object
    candidate = m.group(0) if m else raw
    repaired = re.sub(r",\s*([}\]])", r"\1", candidate)  # drop trailing commas
    for c in (candidate, repaired):
        try:
            return json.loads(c)
        except Exception:
            continue
    return None


def _ask_json(system: str, user: str, attempts: int = 3) -> dict:
    """Never raises. Retries the LLM if it returns unparseable JSON; gives up with {}
    so callers can fall back instead of crashing the whole request."""
    u = user
    for i in range(attempts):
        try:
            resp = _with_retry(lambda: G.client.chat.completions.create(
                model=G.CHAT_MODEL,
                messages=[{"role": "system", "content": system},
                          {"role": "user", "content": u}],
                temperature=0,
                response_format={"type": "json_object"},
            ))
            parsed = _safe_json(resp.choices[0].message.content)
            if isinstance(parsed, (dict, list)):
                return parsed
        except Exception as e:
            print(f"[_ask_json] attempt {i+1} failed: {e}")
        u = user + "\n\nReturn ONLY a single valid JSON object. No prose, no code fences, escape all strings."
    return {}


# ─────────────────────────────────────────────────────────────────────────
# STAGE 1 — ROUTER
# ─────────────────────────────────────────────────────────────────────────
ROUTER_SYS = """You are the router for the WEBXPAY payment-gateway analytics assistant.
Decide how to answer. Return ONLY JSON:
{"lane":"db|web|hybrid|chit_chat","standalone_question":"...","reason":"..."}
- db  = anything about internal data: merchants, GMV, revenue, MDR, volume, POS, IPG,
        transactions, payment status, currencies, onboarding, comparisons, trends.
- web = public/external info: regulations, competitors, market news, external standards.
- chit_chat = greetings/thanks.
Internal numbers always come from the database.

FOLLOW-UPS: if CONVERSATION SO FAR is given, the new question may depend on it
("above is ipg or pos", "what about POS?", "and last year?", "break it down"). Rewrite
standalone_question to be FULLY self-contained by pulling the metric, period, channel and
subject from the previous turns. Example: previous "how many merchants onboarded last month"
+ new "above is ipg or pos" -> standalone_question:
"For the merchants onboarded last month, how many are IPG vs POS?" Keep lane = db."""

def route(question: str, history=None) -> dict:
    ctx = ""
    if history:
        # most-recent-last; include the last few turns so 'above'/'that' resolve
        lines = [f'{m.get("role","user")}: {m.get("content","")}' for m in history[-6:]]
        ctx = "CONVERSATION SO FAR (oldest first):\n" + "\n".join(lines) + "\n\n"
    out = _ask_json(ROUTER_SYS, f"{ctx}NEW QUESTION: {question}")
    if isinstance(out, list):
        out = next((x for x in out if isinstance(x, dict)), {})
    return out if isinstance(out, dict) else {"lane": "db", "standalone_question": question}


# ─────────────────────────────────────────────────────────────────────────
# STAGE 2 — PLANNER
# ─────────────────────────────────────────────────────────────────────────
PLANNER_SYS = f"""You are the query planner for WEBXPAY (Sri Lankan payment gateway).
Turn the question into a PLAN. Do NOT write SQL. Return ONLY JSON:
{{"tool":"...","params":{{...}},"channel":"ipg|pos|both",
  "requested_output":"summary|detail_rows|comparison|trend|ranking|explanation|recommendation",
  "assumptions":["..."]}}

Tools (what each COVERS):
- merchant_onboarding: ONLY counts/lists merchants that registered in a period. It does NOT
  check whether they transacted.
- pos_summary / ipg_summary / combined_summary: GMV/revenue/volume totals for a period.
- timeseries: a metric broken down by day/week/month. txn_status: approved/declined/etc counts.
- top_merchants: ranking. period_comparison: metric in period A vs period B. gmv_drop_diagnosis: "why did GMV drop".
- non_transacting_merchants: merchants that did NOT transact in a period (dormant / inactive /
  "no transactions" / "non-transacting" / "zero transactions"). Uses validated dedup logic.
- active_transacting_merchants: merchants that DID transact in a period (active / transacting),
  optionally by channel.
- merchant_type: classify/count merchants by which channel they USE (registration-level, not
  transactions): "IPG only", "POS only", "both IPG and POS". Put the filter in
  params.filter = "ipg" | "pos" | "both" | "all".
- merchant_movers: SIMPLE "which merchants dropped/improved between two months" ONLY.
  Use it when the ONLY ask is the list of movers ("which merchants dropped more than 10% in
  June vs May", "who improved the most month over month", "why did GMV drop in June vs May").
  DO NOT use it if the question ALSO asks for extra breakdowns — channels combined AND separate,
  grouping by MCC/category, or a chart. Those go to adhoc_sql (it can build the full analysis).
- gmv_drop_diagnosis: "why did GMV drop / change between two specific DAYS" (day-level only,
  e.g. "why was the 11th lower than the 12th").
- CURRENT RULE: use merchant_movers for month-vs-month merchant mover questions even when
  the user asks for RM name, combined/IPG/POS columns, MCC/category grouping, or a chart.
- adhoc_sql: anything that COMBINES concepts or has no dedicated tool — e.g. "did the merchants
  onboarded last month transact?", "merchants who signed up but never paid", "loss for a month"
  (loss = negative revenue). If the question mixes a merchant GROUP with an ACTIVITY check,
  use adhoc_sql (NOT merchant_onboarding). ALSO use adhoc_sql for COMPLEX drop/comparison
  analyses that need several things at once — e.g. "merchants who dropped >10% June vs May,
  IPG and POS combined AND separately, categorized by MCC" — merchant_movers can't do those.

Rules:
- Resolve ALL dates to explicit YYYY-MM-DD. Emit params.date_start (inclusive) and
  params.date_end (exclusive). Today is {date.today().isoformat()}.
- IPG = online (tbl_order). POS = terminals (tbl_pos_transactions, providers DFCC/HNB).
  If no channel is stated, channel="both".
- requested_output must match what was literally asked
  ("total"->summary, "list/show each"->detail_rows, "compare"->comparison,
   "trend/monthly"->trend, "top/worst"->ranking, "why"->explanation).
- Pick the tool by what it COVERS, not by keywords in the question. A question containing
  "onboarded" is NOT automatically merchant_onboarding — if it also asks about transactions/
  activity/revenue of those merchants, use adhoc_sql."""

def _fallback_plan(question: str) -> dict:
    """Keyword-based plan used when the LLM planner fails/returns junk — mirrors the old
    system's routing so the question still gets answered instead of crashing."""
    ql = question.lower()
    intent = G.analyze_intent(question)
    params = {"date_start": intent.get("date_start"), "date_end": intent.get("date_end")}
    ch = "pos" if "pos" in ql else ("ipg" if "ipg" in ql else "both")

    def mk(tool, **extra):
        return {"tool": tool, "params": params, "channel": ch,
                "requested_output": extra.get("out", "summary"), "assumptions": ["keyword fallback (planner JSON failed)"]}

    if any(k in ql for k in ("non transact", "non-transact", "not transact", "no transaction", "zero transact", "dormant", "inactive merchant")):
        return mk("non_transacting_merchants", out="detail_rows")
    if any(k in ql for k in ("onboard", "signed up", "registered", "new merchant")):
        return mk("merchant_onboarding", out="summary")
    if any(k in ql for k in ("ipg only", "pos only", "both ipg", "both pos", "merchant type", "which channel")):
        return mk("merchant_type", out="detail_rows")
    if any(k in ql for k in ("approved", "declined", "failed", "abandoned", "cancelled")):
        return mk("txn_status", out="summary")
    if any(k in ql for k in ("top ", "rank", "highest", "lowest", "best ", "worst ")):
        return mk("top_merchants", out="ranking")
    if intent.get("time_grain"):
        return mk("timeseries", out="trend")
    if intent.get("type") in ("gmv", "revenue", "volume", "mdr"):
        return mk("combined_summary" if ch == "both" else (f"{ch}_summary"), out="summary")
    return mk("adhoc_sql", out="summary")


def plan(question: str) -> dict:
    out = _ask_json(PLANNER_SYS, f"Question: {question}")
    # The LLM sometimes wraps the plan in a JSON array, or nests it under a key.
    # Normalise to a single plan dict so downstream code can rely on .get().
    if isinstance(out, list):
        out = next((x for x in out if isinstance(x, dict)), {})
    if isinstance(out, dict) and "tool" not in out:
        for v in out.values():                       # e.g. {"plan": {...}}
            if isinstance(v, dict) and "tool" in v:
                out = v
                break
    # If the planner produced nothing usable, fall back to keyword routing (never crash).
    if not (isinstance(out, dict) and out.get("tool")):
        print("[plan] LLM plan unusable -> keyword fallback")
        return _fallback_plan(question)
    return out


# ─────────────────────────────────────────────────────────────────────────
# STAGE 3 — build SQL from the plan, REUSING your existing builders
# ─────────────────────────────────────────────────────────────────────────
def build_sql(question: str, pl: dict) -> str:
    if not isinstance(pl, dict):
        pl = {}
    # Store + Relationship Manager (RM) name — fixed cross-db join to merchant_db.
    if any(k in question.lower() for k in ("rm name", "rm names", "relationship manager",
            "which rm", "rm for ", "rm of ", "manager name", "stores with rm",
            "store with rm", "merchant rm", "rm code", "signup rm")):
        return G.build_store_rm_sql(question)
    tool = pl.get("tool", "adhoc_sql")
    p = pl.get("params", {})
    ds, de = p.get("date_start"), p.get("date_end")
    ch = pl.get("channel", "both")
    intent = G.analyze_intent(question)   # reuse your date/metric extraction as a fallback

    if tool == "pos_summary":
        return G.build_pos_sql({**intent, "channel": "pos"})
    if tool in ("ipg_summary", "combined_summary"):
        return G.build_gmv_sql(question, intent)
    if tool == "timeseries":
        grain = p.get("grain") or p.get("interval") or "month"
        metric = (p.get("metric") or "").lower()
        if ch == "pos":
            return G.build_pos_timeseries_sql(ds, de, grain)
        # GMV-only trend -> lean query (exact GMV formula, but no revenue/MDR/opg overhead
        # that triples the per-row FX work and can time out).
        if metric in ("gmv", "sales", "volume", "") and ch != "pos":
            return build_ipg_gmv_trend_sql(ds, de, grain)
        return G.build_ipg_timeseries_sql(ds, de, grain)
    if tool == "txn_status":
        return G.build_txn_status_sql(question, ds, de)
    if tool == "merchant_onboarding":
        return G.build_merchant_onboarding_sql(question, ds, de)
    if tool == "non_transacting_merchants":
        # NOTE: the FAST path (local pre-aggregated mart) is handled upstream in
        # engine.py as a Python handler, because it combines MySQL + local SQLite and
        # can't be a single cross-database SQL statement. This is the heavy fallback,
        # used only when the mart hasn't been backfilled yet.
        _sql = G.build_business_sql(question, intent)
        return _sql if _sql else generate_adhoc_sql(question, pl)
    if tool == "active_transacting_merchants":
        return G.build_active_transacting_merchants_sql(question, ds, de)
    if tool == "merchant_type":
        ft = (p.get("filter") or "all").lower()
        if ft not in ("ipg", "pos", "both", "all"):
            ft = "all"
        want_count = (pl.get("requested_output") == "summary"
                      or any(w in question.lower() for w in ("how many", "count", "number of")))
        return G.build_merchant_type_count_sql(ft) if want_count else G.build_merchant_type_sql(ft)
    if tool in ("merchant_movers", "gmv_drop_diagnosis"):
        # We only reach here if the full handler in engine.py DECLINED (returned None) —
        # e.g. a complex drop analysis it can't do. Use the AI writer, NOT G.generate_sql
        # (whose keyword routing would mis-handle "ipg and pos" as a merchant-type list).
        return generate_adhoc_sql(question, pl)
    if tool == "adhoc_sql":
        # No canonical builder fits (e.g. "did the onboarded merchants transact?").
        # Use the clean AI SQL writer — NOT G.generate_sql, whose keyword routing would
        # hijack anything containing "onboard"/"revenue"/etc. into a fixed builder.
        # Pass the plan so the writer uses the RESOLVED dates instead of guessing the year.
        return generate_adhoc_sql(question, pl)
    # any other planner tool not wired here -> existing generator
    return G.generate_sql(question, G.load_schema())


# ─────────────────────────────────────────────────────────────────────────
# ADHOC SQL WRITER — clean LLM SQL for questions no canonical builder covers.
# Carries the business definitions so the LLM writes correct joins/filters.
# ─────────────────────────────────────────────────────────────────────────
ADHOC_SQL_SYS = """You are a senior MySQL engineer for WEBXPAY (Sri Lankan payment gateway).
Write EXACTLY ONE read-only MySQL SELECT that answers the question. Output only ```sql ... ```.

HARD RULES (a validator will reject violations):
- SELECT/WITH only. No DML/DDL. Never SELECT *. Add LIMIT unless it's a single aggregate.
- Use ONLY tables/columns in the SCHEMA. Every JOIN needs an ON clause. Guard ratios with NULLIF.
- Dates half-open: col >= 'YYYY-MM-DD' AND col < 'YYYY-MM-DD'.

PERFORMANCE (CRITICAL — queries are killed after 30s):
- ALWAYS put the date filter in the WHERE of the CTE/subquery that READS a transaction table.
  NEVER select all history and filter later with CASE WHEN / HAVING. Every CTE or subquery that
  touches tbl_order, tbl_payment, or tbl_pos_transactions MUST have its own date-range WHERE so
  it scans only the needed window (that's what lets the date indexes work).
- For a two-period comparison, filter each transaction CTE to the FULL span covering BOTH
  periods (earliest start .. latest end), then split into periods with CASE WHEN inside the
  aggregate. Read the minimum rows possible.

BUSINESS DEFINITIONS (use exactly):
- Merchant = tbl_store (s). Name = s.doing_business_name. WEBXPAY is the company, not a merchant.
- Onboard date = COALESCE(s.credit_review_approved_date, s.date_registered).
- IPG transaction = row in tbl_order (o) joined to tbl_payment (p) on o.payment_id = p.payment_id;
  APPROVED = o.payment_status_id = 2; IPG date = p.date_time_transaction; link to merchant via o.store_id = s.store_id.
- POS transaction = row in tbl_pos_transactions (t); APPROVED = LOWER(TRIM(COALESCE(t.txn_type,''))) IN ('sale','amex')
  AND t.currency='LKR' AND t.ipg_provider_id IN (5,6); POS date = t.transaction_date; link via t.store_id = s.store_id.
- "Did a merchant transact" = EXISTS an approved IPG order OR an approved POS transaction for that store_id.
- RM/store signup details live in merchant_db. When RM name is requested, join:
  merchant_db.wbx_merchant_signups ms ON ms.merchant_id = s.store_id;
  merchant_db.wbx_live_rms lr ON lr.id = ms.live_rm_id;
  merchant_db.wbx_admin_users au ON au.id = COALESCE(ms.admin_user_id, lr.admin_user_id).
  Select au.name AS rm_name. Store name still comes from webxpay_master.tbl_store.doing_business_name.
- Revenue (IPG) = SUM(o.total_amount * (o.payment_gateway_rate - o.bank_payment_gateway_rate)/100)
  over approved orders. LOSS = the portion where that margin is NEGATIVE (merchant rate below
  bank/cost rate). Report loss as a negative revenue figure (SUM of only the negative rows).
- Transaction/store tables live in schema webxpay_master; RM/signup/admin-user tables live in schema merchant_db.
  Prefix table names with their schema as the existing queries do.

SCHEMA:
{schema}
"""

def build_ipg_gmv_trend_sql(ds: str, de: str, grain: str = "month") -> str:
    """Lean IPG GMV trend: EXACT validated GMV formula + volume only. Drops the
    revenue/MDR columns and the opg join from build_ipg_timeseries_sql — those triple the
    per-row exchange-rate work and cause timeouts on longer ranges."""
    fmt = {"day": "%Y-%m-%d", "week": "%x-W%v", "month": "%Y-%m"}.get(grain, "%Y-%m")
    label = {"day": "day", "week": "year_week", "month": "year_month"}.get(grain, "year_month")
    gmv_expr = """
        CASE
          WHEN o.processing_currency_id = '5' THEN o.total_amount
          WHEN o.exchange_rate IS NOT NULL AND o.exchange_rate NOT LIKE ''
               AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
               THEN o.total_amount * o.exchange_rate
          ELSE o.total_amount * (
            SELECT er.buying_rate FROM webxpay_master.tbl_exchange_rate er
            WHERE er.currency_id = o.processing_currency_id
              AND er.date <= DATE(p.date_time_transaction)
            ORDER BY er.date DESC LIMIT 1)
        END""".strip()
    return f"""
SELECT
  DATE_FORMAT(p.date_time_transaction, '{fmt}') AS `{label}`,
  ROUND(SUM(CASE WHEN o.payment_status_id = 2 AND o.processing_currency_id IN ('5','2')
                 THEN {gmv_expr} ELSE 0 END), 2) AS `ipg_gmv_lkr`,
  SUM(CASE WHEN o.payment_status_id = 2 THEN 1 ELSE 0 END) AS `ipg_volume`
FROM webxpay_master.tbl_order o
JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
WHERE p.date_time_transaction >= '{ds}' AND p.date_time_transaction < '{de}'
GROUP BY DATE_FORMAT(p.date_time_transaction, '{fmt}')
ORDER BY DATE_FORMAT(p.date_time_transaction, '{fmt}') ASC;
""".strip()


def _flatten_dates(obj) -> list[str]:
    """Pull every YYYY-MM-DD string out of a (possibly nested) plan-params object."""
    import re as _re
    out = []
    if isinstance(obj, str):
        if _re.fullmatch(r"\d{4}-\d{2}-\d{2}", obj):
            out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            out += _flatten_dates(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            out += _flatten_dates(v)
    return out


def generate_adhoc_sql(question: str, pl: dict | None = None) -> str:
    try:
        from schema_loader import get_schema_text
        schema = get_schema_text()
    except Exception:
        schema = G.load_schema()

    pl = pl or {}
    params = pl.get("params", {}) if isinstance(pl, dict) else {}
    # Collect every resolved date so we can tell the writer the exact overall window to
    # push into its transaction CTEs (prevents full-history scans).
    _dates = sorted({v for v in _flatten_dates(params)})
    _span = ""
    if _dates:
        _span = (f"\nRESTRICT every transaction-reading CTE/subquery with a WHERE date filter to "
                 f"the window >= '{_dates[0]}' AND < '{_dates[-1]}' (the full span of the requested "
                 f"periods). Do NOT scan outside this window.")
    date_ctx = (
        f"\nTODAY IS {date.today().isoformat()}. Resolve any relative period against today.\n"
        f"The planner already resolved these values — USE THEM EXACTLY, do not invent a year:\n"
        f"  params: {params}\n"
        f"  channel: {pl.get('channel')}   requested_output: {pl.get('requested_output')}\n"
        "If the question compares two periods (e.g. 'May vs June'), use the current year "
        "unless the question states otherwise. NEVER hardcode a past year like 2023."
        + _span
    )
    resp = _with_retry(lambda: G.client.chat.completions.create(
        model=G.CHAT_MODEL,
        messages=[{"role": "system", "content": ADHOC_SQL_SYS.replace("{schema}", schema)},
                  {"role": "user", "content": f"QUESTION: {question}\n{date_ctx}"}],
        temperature=0.1,
    ))
    return G.extract_sql_from_text(resp.choices[0].message.content or "")


# ─────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────
def main():
    args = [a for a in sys.argv[1:] if a != "--execute"]
    execute = "--execute" in sys.argv
    question = " ".join(args) or "what was the IPG revenue for March 2025?"

    print(f"\nQUESTION: {question}\n" + "=" * 70)

    print("\n[1] ROUTER — where should this go?")
    r = route(question)
    print(json.dumps(r, indent=2))
    q = r.get("standalone_question") or question

    if r.get("lane") != "db":
        print(f"\n-> lane is '{r.get('lane')}' — not a database question. (web/greeting handled elsewhere)")
        return

    print("\n[2] PLANNER — what does the user actually want?")
    pl = plan(q)
    print(json.dumps(pl, indent=2))

    print("\n[3] SQL — chosen tool builds the query (your existing, validated builder):")
    sql = build_sql(q, pl)
    print(sql)

    print("\n[4] VALIDATE — safety checker:")
    print(f"   (schema source: {schema_source()})")
    ok, safe_sql, problems = validate_sql(sql, get_schema_tables(),
                                          max_rows=pl.get("params", {}).get("limit", 1000))
    print(f"   approved: {ok}")
    print(f"   notes: {problems or 'none'}")

    if not ok:
        print("\n[BLOCKED] Query was blocked by the safety checker — would NOT run.")
        return

    safe_sql = with_timeout_hint(safe_sql)
    print("\n[OK] This is the query that WOULD run:")
    print(safe_sql)

    if execute:
        print("\n[5] EXECUTE against the database…")
        from db import run_sql
        rows = run_sql(safe_sql)
        print(json.dumps(rows[:10] if isinstance(rows, list) else rows, indent=2, default=str))

        print("\n[6] ANSWER — plain-English explanation:")
        from answer import explain
        if isinstance(rows, dict) and "error" in rows:
            print(f"   database error: {rows['error']}")
        else:
            print(explain(q, rows, pl))
    else:
        print("\n(dry-run: not executed. Re-run with --execute to run + explain.)")


if __name__ == "__main__":
    main()
