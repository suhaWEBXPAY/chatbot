"""
Agentic SQL engine — answers ANY question by reasoning over the real schema.

Instead of keyword-matching the question into a pre-built SQL template, this
engine gives the LLM the full schema + business rules and lets it INVESTIGATE:
it can run several read-only queries (resolve how a person/merchant name is
actually spelled, inspect distinct values, then compute the real answer)
before writing a final answer grounded ONLY in rows that came back from the DB.

Used by gpt_helpers.handle_user_question in two places:
  1. route_question() decides up-front whether a question needs this engine
     (people/RMs, POS machines, judgment questions, unusual filters, lookups).
  2. answer_with_agent() is also the last-resort fallback when the legacy
     template pipeline returns an error or zero rows.

Canonical metric questions (GMV / revenue / MDR / volume / trends / top
merchants) are deliberately NOT handled here — the validated legacy builders
keep those numbers exactly matching Power BI.
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

_client = OpenAI(
    api_key=os.getenv("GEMINI_API_KEY"),
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
)
_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

_MAX_QUERIES = 6      # SQL budget per question
_MAX_TURNS = 10       # LLM-call budget (queries + retries + final)
_MAX_SECONDS = 150    # wall-clock budget for the whole investigation
_QUERY_TIMEOUT_MS = 45000  # per-query cap (vs run_sql's 3-min session cap)
_OBS_ROWS = 50        # rows shown back to the model per query
_OBS_CHARS = 7000     # hard cap on observation text sent to the model
_RESULT_ROWS = 500    # rows returned to the frontend table


def _load_schema() -> str:
    try:
        p = os.path.join(os.path.dirname(__file__), "schema.txt")
        with open(p, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


# =========================================================
# ROUTER — does this question need the agent?
# =========================================================
_ROUTER_PROMPT = """You route questions for the WEBXPAY analytics chatbot. Reply with EXACTLY one word: metric OR agent.

metric = canonical company-wide metric questions the validated pipeline already computes:
totals/trends/breakdowns of GMV, revenue, MDR, transaction volume/count, approved/declined/abandoned
status counts, daily/weekly/monthly timeseries, period overviews and period comparisons, top/bottom
merchants by GMV or revenue, merchant counts by channel (IPG/POS/both), active/non-transacting merchant
lists, how many merchants were onboarded in a period.

agent = everything else — any question that needs looking things up or multi-step reasoning:
- mentions a specific PERSON, relationship manager (RM), salesperson, or asks "who ..."
- POS machines / terminals / devices
- PAYMENT GATEWAYS: gateway-wise breakdowns/rankings/profitability, named gateways (JustPay...)
- metric questions with a NON-STANDARD filter (by RM, by city, by category, by bank, by card type...)
- judgment/quality questions ("good merchants", "performing well", "worth keeping"),
  "analyze X and suggest improvements" (compute the real numbers, then advise)
- questions about a specific named merchant/store
- customers, banks, categories, countries, currencies as entities
- anything vague, multi-hop, or not clearly covered by the metric list above

Examples:
"pos gmv may 2026" -> metric
"monthly gmv trend for 2025" -> metric
"top 10 merchants by revenue last month" -> metric
"how many merchants were onboarded last month" -> metric
"total approved transactions in june" -> metric
"who was onboarded by erandi last month" -> agent
"how many active pos machines are there" -> agent
"did rushda bring in good merchants" -> agent
"gateway wise transactions in june" -> agent
"which gateways should we scale" -> agent
"analyze our decline rate and suggest improvements" -> agent
"which city has the most merchants" -> agent
"what is keells' total sales this year" -> agent
"which rm onboarded the most merchants in 2026" -> agent

Reply with one word only."""


# Deterministic backup for when the LLM router itself is down (e.g. Gemini 503
# "high demand"): person/RM/terminal questions must NOT fall into the legacy
# keyword templates — those have no RM logic and return unrelated tables.
_PERSON_HINTS = re.compile(
    r"(?i)\brms?\b|relationship manager|salesperson|sales rep|\bwho\b|"
    r"onboarded by|best rm|\bterminals?\b|pos machine|\bgateways?\b|gateway.wise")


def needs_agent_heuristic(question: str) -> bool:
    return bool(_PERSON_HINTS.search(question or ""))


def _chat(**kwargs):
    """LLM call with backoff retries — Gemini intermittently 503s under load
    (sometimes for a minute or more), and a failed router/agent call otherwise
    derails the whole question. Only transient errors (503/429) are retried."""
    _delays = (2, 5, 10)
    for _i, _delay in enumerate(_delays):
        try:
            return _client.chat.completions.create(**kwargs)
        except Exception as e:
            _es = str(e)
            _transient = any(t in _es for t in ("503", "UNAVAILABLE", "429",
                                                "overloaded", "high demand"))
            if not _transient:
                raise
            print(f"[agent_engine] LLM call failed ({e}) — retry "
                  f"{_i + 1}/{len(_delays)} in {_delay}s")
            time.sleep(_delay)
    return _client.chat.completions.create(**kwargs)


def route_question(question: str) -> str:
    """Returns 'agent' or 'metric'. If the router LLM is unreachable, falls back
    to the keyword heuristic (NOT blindly to legacy — see _PERSON_HINTS)."""
    try:
        resp = _chat(
            model=_MODEL,
            temperature=0,
            max_tokens=8,
            reasoning_effort="none",  # disable Gemini thinking; else it eats the token budget
            messages=[
                {"role": "system", "content": _ROUTER_PROMPT},
                {"role": "user", "content": question},
            ],
        )
        label = (resp.choices[0].message.content or "").strip().lower()
        return "agent" if "agent" in label else "metric"
    except Exception as e:
        fallback = "agent" if needs_agent_heuristic(question) else "metric"
        print(f"[agent_engine] router failed ({e}) -> heuristic: {fallback}")
        return fallback


# =========================================================
# SYSTEM PROMPT (schema + semantic layer + investigation rules)
# =========================================================
_BUSINESS_RULES = """BUSINESS RULES (semantic layer — trust these over guesses):

Databases: two MySQL databases on the same server. `webxpay_master` is the default
(its tables can be used unqualified). ALWAYS prefix the RM/signup tables with
`merchant_db.` (e.g. merchant_db.wbx_admin_users).

Terminology — volume vs value:
- "transaction volume" / "number of transactions" = COUNT of transactions.
- "transaction value" / GMV = SUM of amounts.
- Rank by the metric actually asked: "highest volume" = highest COUNT, NOT highest
  GMV (showing both columns is good, but the headline answer must use the right one).

IPG (online / internet payment gateway) transactions:
- tbl_order o JOIN tbl_payment p ON p.payment_id = o.payment_id
- o.payment_status_id: 1 = Abandoned, 2 = Approved/successful, 3 = Declined, 4 = Cancelled.
  decline rate = declined / (approved + declined); abandonment rate = abandoned / all attempts.
- ABANDONED orders usually have NO tbl_payment row — the o JOIN p drops them, making the
  abandon count falsely 0. Count abandoned from tbl_order ALONE with o.date_added as the
  date (LEFT JOIN p if payment fields are needed).
- transaction time = p.date_time_transaction
- amount = o.total_amount, in o.processing_currency_id (5=LKR, 2=USD, 1=GBP, 3=EUR, 6=AUD)
- LKR conversion: currency 5 -> amount as-is; else if o.exchange_rate is a plain number
  (REGEXP '^[0-9]+(\\.[0-9]+)?$') use amount * o.exchange_rate; else use the latest
  tbl_exchange_rate.buying_rate for that currency with date <= the transaction date.

POS (physical card machine) transactions:
- tbl_pos_transactions t ; date column = t.transaction_date ; ipg_provider_id 5 = HNB, 6 = DFCC.
- real sales = txn_type IN ('sale','amex') AND currency = 'LKR'. Rows with txn_type LIKE 'void%'
  cancel a matching sale. For simple activity/count questions you may count sales and say voids
  were ignored; exact GMV uses void-pair elimination (the canonical pipeline handles that).
- A POS machine/terminal = a DISTINCT t.terminal_id (t.terminal_sn is the hardware serial).
  "Active machines" = terminals with >= 1 transaction in a recent window; default to the last
  90 days AND also report the last-30-day figure, and state the window in the answer.
- tbl_pos_store_bank_mid maps store_id <-> bank_merchant_mid and holds mdr_rate / cost_rate.
- POS REVENUE (the validated formula — matches Power BI): per sale/amex row,
    t.amount * (COALESCE(t.mdr_rate,0) - COALESCE(m.cost_rate,1.7)) / 100
  with the deduped cost join:
    LEFT JOIN (SELECT store_id, bank_merchant_mid, MAX(cost_rate) AS cost_rate
               FROM tbl_pos_store_bank_mid WHERE cost_rate IS NOT NULL
               GROUP BY store_id, bank_merchant_mid) m
      ON m.store_id = t.store_id AND m.bank_merchant_mid = t.bank_merchant_mid
  (NO is_active filter on the MID table). Exclude void rows (txn_type LIKE 'void%');
  the canonical pipeline also pair-eliminates voided sales — state that small caveat
  if you skip pair elimination. Single-day POS revenue via this is cheap on live MySQL.

Merchants / stores:
- tbl_store s ; display name = s.doing_business_name (fallback s.registered_name).
- active merchant = s.is_active = 1 AND s.free_trail = 0 (note the column really is
  spelled "free_trail"). Onboarding/registration date = s.date_registered
  (s.active_date = when they went live).
- a merchant has POS if it appears in tbl_pos_transactions (or tbl_pos_store_bank_mid);
  has IPG if it has rows in tbl_order.

Relationship Managers (RM = the salesperson who onboards/brings in merchants):
- RM names live in merchant_db.wbx_admin_users.name. Authoritative join chain from a store:
    tbl_store.store_id = merchant_db.wbx_merchants.merchant_id
    merchant_db.wbx_merchants.id = merchant_db.wbx_merchant_signups.merchant_id
        (CAREFUL: wbx_merchant_signups.merchant_id references wbx_merchants.id, NOT store_id)
    merchant_db.wbx_merchant_signups.live_rm_id = merchant_db.wbx_live_rms.id
    merchant_db.wbx_live_rms.admin_user_id = merchant_db.wbx_admin_users.id
- tbl_store.signup_rm_name and tbl_store.rm_code are denormalized copies — fine for quick
  lookups, but prefer the join chain when attributing merchants to an RM.
- "onboarded by X last month" = stores whose RM resolves to X with tbl_store.date_registered
  inside that month.
- RM "sales"/"GMV"/"performance" = the approved GMV of that RM's merchants across BOTH
  channels — IPG (tbl_order approved, LKR-converted) PLUS POS (tbl_pos_transactions LKR
  sale/amex) — unless the user explicitly restricts to one channel. Some RMs are almost
  entirely POS (an IPG-only ranking can show a top RM near zero), so a single-channel
  ranking is misleading. Compute the channels as separate small queries if needed, and
  ALWAYS state which channels the final figures include.

Payment gateways (IPG "gateway-wise" analysis — use EXACTLY this chain):
- Gateway of an IPG order: tbl_order.store_payment_gateway_id
    -> tbl_store_payment_gateway_2.store_payment_gateway_id
    -> tbl_store_payment_gateway_2.payment_gateway_id -> tbl_payment_gateway.
  Gateway NAME = tbl_payment_gateway.display ; enabled flag = tbl_payment_gateway.active
  (this table uses `active`, NOT is_active). Bank/provider = tbl_payment_gateway.ipg_provider_id
    -> tbl_ipg_provider.ipg_provider.
- Date filter for gateway-wise reports: o.date_added (the ORDER date — matches the company's
  gateway report exactly; no tbl_payment join needed unless you want payment-time fields).
- NEVER attribute gateways through tbl_order_parent_gateway — only a small subset of orders
  has rows there (an analysis that used it left 94% of transactions "unattributed", which is
  wrong). tbl_order_parent_gateway is ONLY for the parent-gateway RATE in the revenue formula.
- SANITY: gateway-wise transaction counts must add up to roughly ALL approved transactions
  for the period. If a large share comes back "unattributed", your join is wrong — fix it
  before answering.
- IPG PROFIT/revenue (the validated formula — convenience fees are NOT the profit
  metric; revenue comes from the MDR rate spread). COPY THIS TEMPLATE EXACTLY — do NOT
  simplify the FX conversion or the rate CASE (a simplified version once under-reported
  revenue by 44%):
    SELECT ROUND(SUM(
      (CASE
         WHEN o.processing_currency_id = '5' THEN o.total_amount
         WHEN o.exchange_rate IS NOT NULL AND o.exchange_rate NOT LIKE ''
              AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
           THEN o.total_amount * o.exchange_rate
         ELSE o.total_amount * (SELECT er.buying_rate FROM tbl_exchange_rate er
                                WHERE er.currency_id = o.processing_currency_id
                                  AND er.date <= DATE(p.date_time_transaction)
                                ORDER BY er.date DESC LIMIT 1)
       END)
      * (o.payment_gateway_rate
         - CASE WHEN o.order_type_id = 3
                THEN CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4)) + COALESCE(opg.parent_gateway_rate,0)
                ELSE CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4)) END) / 100
    ), 2) AS ipg_revenue_lkr
    FROM tbl_order o
    JOIN tbl_payment p ON p.payment_id = o.payment_id
    LEFT JOIN tbl_order_parent_gateway opg ON opg.order_id = o.order_id
    WHERE o.payment_status_id = 2
      AND p.date_time_transaction >= '<start>' AND p.date_time_transaction < '<end>'
  Group by the gateway chain above for gateway-wise profitability. Keep the date window
  small (a day/week/month) — this is a live-table scan.

Judgment / quality questions ("good merchants", "performing well"):
- pick measurable criteria (e.g. total approved GMV since onboarding, transacting recently,
  is_active), compute them, and STATE the criteria you used in the answer."""


def _learned_rules() -> str:
    try:
        from learning_store import lessons_block
        return lessons_block()
    except Exception:
        return ""


def _mart_section() -> str:
    """Describes the local pre-computed mart to the agent (with live coverage
    dates and the company-wide YTD total as a sanity anchor), or "" when the
    mart isn't built."""
    try:
        from redesign.pos_summary_mart import mart_coverage, mart_query
        cov = mart_coverage()
    except Exception:
        cov = {}
    if not cov:
        return ""
    year = datetime.now().year
    company_ytd = ""
    try:
        r = mart_query(f"""SELECT
            (SELECT COALESCE(SUM(gmv),0) FROM ipg_daily_gmv WHERE activity_date >= '{year}-01-01') +
            (SELECT COALESCE(SUM(valid_sale_amount),0) FROM pos_daily_activity WHERE activity_date >= '{year}-01-01') AS t""")
        if isinstance(r, list) and r:
            company_ytd = f"{float(r[0]['t']):,.0f}"
    except Exception:
        pass
    return f"""FAST LOCAL MART (action "mart_sql" — SQLite dialect, answers in milliseconds):
A local pre-computed mart holds validated daily GMV per store. For ANY heavy aggregation —
RM performance/rankings, top merchants, multi-month totals or trends, per-merchant history,
channel comparisons — you MUST use the mart FIRST via {{"action": "mart_sql", "sql": "..."}}.
Live MySQL scans of tbl_order/tbl_pos_transactions for such questions time out.

Mart tables (SQLite — use substr()/strftime(), julianday(); NOT MONTH()/DATE_FORMAT):
- ipg_daily_gmv(store_id, activity_date 'YYYY-MM-DD', gmv, txn_count)
    approved IPG GMV per store per day, already LKR-converted (validated FX logic).
    Coverage: {cov.get('ipg_from')} to {cov.get('ipg_to')}.
- pos_daily_activity(store_id, activity_date, ipg_provider_id (5=HNB, 6=DFCC),
    valid_sale_count, valid_sale_amount)
    void-deduped POS sales per store per day (validated pair-elimination logic);
    valid_sale_amount = POS GMV in LKR. Coverage: {cov.get('pos_from')} to {cov.get('pos_to')}.
- store_dim(store_id, merchant_name, registered_name, is_active, free_trail,
    date_registered, rm_name, has_ipg, has_pos)
    EXACTLY ONE row per store incl. the resolved Relationship Manager ({cov.get('stores')} stores).
    has_ipg / has_pos = 1 when the store has an active IPG gateway / POS MID ("subscriptions").
    Merchant counts per RM, channel-subscription counts, active-merchant counts: ALWAYS from
    store_dim (COUNT(*)/SUM(has_ipg)/SUM(has_pos) GROUP BY rm_name) — never from live
    merchant_db tables.

Example — RM ranking by combined GMV for June 2026, ONE instant query:
  SELECT d.rm_name,
         ROUND(SUM(COALESCE(i.gmv,0)),2)  AS ipg_gmv_lkr,
         ROUND(SUM(COALESCE(p.amt,0)),2)  AS pos_gmv_lkr,
         ROUND(SUM(COALESCE(i.gmv,0))+SUM(COALESCE(p.amt,0)),2) AS total_gmv_lkr
  FROM store_dim d
  LEFT JOIN (SELECT store_id, SUM(gmv) gmv FROM ipg_daily_gmv
             WHERE activity_date >= '2026-06-01' AND activity_date < '2026-07-01'
             GROUP BY store_id) i ON i.store_id = d.store_id
  LEFT JOIN (SELECT store_id, SUM(valid_sale_amount) amt FROM pos_daily_activity
             WHERE activity_date >= '2026-06-01' AND activity_date < '2026-07-01'
             GROUP BY store_id) p ON p.store_id = d.store_id
  WHERE d.rm_name IS NOT NULL
  GROUP BY d.rm_name HAVING total_gmv_lkr > 0 ORDER BY total_gmv_lkr DESC

Use live MySQL ("action": "sql") only for what the mart does NOT contain: declined/abandoned
status counts, payment gateways, currencies, terminals/POS machines, order-level detail, and
periods outside the coverage dates above. State in your answer that mart data runs through
its coverage end date.

Example — TOP-N MERCHANTS WITH MONTHLY BREAKDOWN (copy this shape; a flat join here
fans out and once inflated one merchant to an impossible Rs 5.8B/month):
  WITH m AS (
    SELECT store_id, substr(activity_date,1,7) AS ym, SUM(gmv) AS g, 0 AS p
    FROM ipg_daily_gmv WHERE activity_date >= '2025-07-01' GROUP BY store_id, ym
    UNION ALL
    SELECT store_id, substr(activity_date,1,7), 0, SUM(valid_sale_amount)
    FROM pos_daily_activity WHERE activity_date >= '2025-07-01' GROUP BY store_id, substr(activity_date,1,7)
  ),
  top_stores AS (
    SELECT store_id FROM m GROUP BY store_id ORDER BY SUM(g + p) DESC LIMIT 20
  )
  SELECT d.merchant_name, m.ym AS month,
         ROUND(SUM(m.g),2) AS ipg_gmv_lkr, ROUND(SUM(m.p),2) AS pos_gmv_lkr,
         ROUND(SUM(m.g)+SUM(m.p),2) AS combined_gmv_lkr
  FROM m JOIN store_dim d ON d.store_id = m.store_id
  WHERE m.store_id IN (SELECT store_id FROM top_stores)
  GROUP BY d.merchant_name, m.ym ORDER BY d.merchant_name, m.ym

Single-RM GMV (resolve the name FIRST, then reuse the two-subquery pattern):
  1. SELECT DISTINCT rm_name FROM store_dim WHERE rm_name LIKE '%pram%'   -- fragment!
  2. same query as the ranking above but with WHERE d.rm_name = '<exact name from step 1>'

Percentage-of-total / shares: compute them IN the SQL so the figures are grounded, e.g.
  ROUND(total_gmv_lkr * 100.0 / SUM(total_gmv_lkr) OVER (), 2) AS pct_of_total
(SQLite window functions are supported on the mart).

MART CAVEATS (each of these has caused a WRONG ANSWER before — follow them exactly):
- NEVER join ipg_daily_gmv and pos_daily_activity directly to store_dim in one flat join:
  pos_daily_activity has MULTIPLE rows per store/day (one per provider), so a flat join
  MULTIPLIES the IPG amounts and inflates GMV massively. ALWAYS pre-aggregate each daily
  table per store in its own subquery (exactly like the example above).
- NEVER join the live merchant_db signup chain (wbx_merchant_signups etc.) onto GMV or
  for counting merchants — it has MULTIPLE rows per merchant and fans out (one RM once
  showed 17B GMV and "2,015 merchants" this way). store_dim already resolved all of it.
- SANITY CHECK before answering (mandatory for RM/merchant aggregations): company-wide
  combined GMV {datetime.now().year} YTD is ~Rs {company_ytd or '9,700,000,000'}, roughly
  1.6B/month. NO single RM or merchant can exceed the company total for the same period,
  and the SUM of all RMs must be <= it. If your numbers break this, the join is fanning
  out — fix the query; presenting it is a critical failure.
- NAME LOOKUPS: match with a SHORT fragment, case-insensitive (LIKE '%pram%', not
  '%Pramoda Desilva%'). If 0 rows, retry with a shorter/different fragment (first 3-4
  letters, or a distinctive later fragment) BEFORE concluding the person doesn't exist —
  user spellings are unreliable ("promada" = Pramoda). Use the USER'S LATEST spelling from
  the conversation, never an earlier misspelling.
- TODAY / intraday questions: the mart is refreshed periodically and can lag live data by
  hours — for "today" use live MySQL with a tight date filter (cheap), or clearly state the
  mart's data-through time.
- "Underperforming/not doing good" questions: NEVER judge merchants on a single day —
  most merchants legitimately have zero on any given day, so an all-zero list is meaningless.
  Compare a recent window instead (e.g. this month's GMV vs the previous month, or last 30
  days vs the prior 30), rank by the drop, and exclude merchants with zero in BOTH windows.
- A result where every merchant shows 0 almost always means the filter/window is wrong —
  investigate before presenting it, and never present an alphabetical zero-list as a ranking."""


def _build_system_prompt() -> str:
    today = datetime.now().strftime("%Y-%m-%d (%A)")
    schema = _load_schema()
    learned = _learned_rules()
    mart = _mart_section()
    mart_action_line = ('  {"thought": "brief reasoning", "action": "mart_sql", "sql": "SELECT ..."}\n'
                        '    -> runs against the FAST LOCAL MART (SQLite) described below, OR\n') if mart else ""
    return f"""You are the WEBXPAY Analytics Agent — an expert MySQL analyst with live READ-ONLY access to the company's databases. Today is {today}. Resolve relative periods yourself ("last month" = the previous calendar month) and always filter dates half-open: >= start AND < end.
Ambiguous numeric dates use the Sri Lankan DD/MM/YYYY convention: "04/07/2026" = 4 July 2026 (NOT April 7). State the resolved date in your answer.

You work in steps. Each turn, reply with EXACTLY ONE JSON object and nothing else:
  {{"thought": "brief reasoning", "action": "sql", "sql": "SELECT ..."}}
    -> the query is executed on live MySQL and the rows are sent back to you, OR
{mart_action_line}  {{"thought": "brief reasoning", "action": "final", "answer": "your markdown answer"}}

INVESTIGATION RULES:
1. One single read-only SELECT (or WITH) statement per turn. Never modify data.
2. When the question names a PERSON (RM/salesperson) or a MERCHANT, FIRST run a small lookup
   query with a case-insensitive LIKE '%name%' to resolve the exact spelling (people:
   merchant_db.wbx_admin_users.name and tbl_store.signup_rm_name; merchants:
   tbl_store.doing_business_name / registered_name). Never assume a spelling. If several
   people match, mention all matches or pick the clearly intended one and say so.
3. Unsure what a column contains? Inspect it first (SELECT DISTINCT ... LIMIT 20).
4. Add LIMIT 200 to any row-listing query. Aggregates (COUNT/SUM/GROUP BY few rows) don't need it.
5. If a query errors or returns something surprising, fix it and retry. You have {_MAX_QUERIES}
   queries total — plan them.
6. Make the LAST query you run the one whose rows best belong in the user's result table
   (the UI shows those rows alongside your answer).
7. If a listing returns EXACTLY as many rows as its LIMIT, the true count is probably larger —
   run a COUNT(*)/aggregate query before stating any total, or say "at least N".
8. Merchant activity lives in BOTH channels: IPG (tbl_order) and POS (tbl_pos_transactions).
   When judging activity, check both — but ALWAYS inside a bounded date window.
8b. LISTING SHAPE: when the question names entities in the PLURAL with a metric
   ("the IPG merchants' GMV for <date>", "gateways' transaction counts"), return ONE ROW
   PER ENTITY (grouped, ranked by the metric) — not just the grand total. State the
   total in your answer too, but the result table must be the per-entity breakdown.
9. PERFORMANCE — queries are killed after ~45 seconds:
   - tbl_order, tbl_payment and tbl_pos_transactions are HUGE. Every scan of them MUST have a
     date filter (p.date_time_transaction / t.transaction_date); never aggregate their full
     history ("since onboarding" -> use last 6 months instead and say so).
   - Prefer 2-3 SMALL aggregate queries over one giant multi-CTE query.
   - If a query times out, do NOT retry a similar heavy one — cut the date window to a single
     month, drop columns, or aggregate first and join later.

SCHEMA:
{schema}

{_BUSINESS_RULES}

{mart}

{learned}

FINAL-ANSWER RULES:
- Every number/name you state MUST come from query results you saw. NEVER estimate, extrapolate
  or invent values. If the data cannot answer, say exactly what is missing.
- Do NOT sum/average subgroups by hand in your answer (e.g. one merchant's 12-month total
  from its monthly rows) — hand-computed figures fail the grounding check. If a subgroup
  total is worth stating, compute it IN SQL first; otherwise describe the pattern in words
  ("consistently around Rs 8-9M/month") and quote individual row values only.
- COUNTS: never state a count/total you did not compute with COUNT(*)/SUM() in a query.
  Do not count returned rows by eye, and never repurpose an ID value from a lookup row as
  if it were a count. If asked "how many X", one of your queries MUST be that COUNT — and
  make it the LAST query so the table shown to the user backs up your number.
- PERIOD HONESTY: today is {today}. If the question's period extends beyond today (e.g. "2026"
  while 2026 is ongoing), the data only covers up to today — describe it as "year-to-date
  (Jan 1 – {today})" and NEVER present a future end date (like December 31) as covered.
- 0 rows is a valid finding — report "none found for <exact filters/period>".
- State the assumptions you made (date window, what "active"/"good" means, which person matched).
- You are the company's analytics assistant speaking about OUR database — never say
  "the data you shared/provided".
- ORGANIZED SUMMARIES, NOT TABLE DUMPS: the UI already shows your last query's rows as a
  table (and auto-renders a chart for multi-row results). NEVER paste the full result table
  into your answer. Structure it instead: (1) one-sentence headline answering the question,
  (2) the top 3-5 entries with their key figures (a SHORT table is fine), (3) notable
  outliers/anomalies worth attention, (4) a final line with definitions/assumptions/period.
- If the user asks for an infographic/chart/visual: the UI renders an interactive chart
  automatically from your final query's rows — make that last query return the rows worth
  charting, and say the chart is shown above. NEVER say you cannot create visuals.
- Use markdown; short tables for lists; format LKR amounts with thousand separators."""


# =========================================================
# GROUNDING GUARD — the final answer may only contain values
# that actually came back from the database.
# =========================================================
_PLACEHOLDER_PAT = re.compile(
    r"(?i)john smith|jane doe|peter jones|alice brown|example table|placeholder|"
    r"sample (?:data|values|results|table)|would be (?:inserted|shown|listed|here)|"
    r"actual (?:results|values|data)[^.]{0,60}(?:inserted|shown|would)|"
    r"for illustration|illustrative|hypothetical")

_DATE_TIME_PAT = re.compile(
    r"\b\d{4}-\d{2}-\d{2}\b|\b\d{4}-\d{2}\b|\b\d{1,2}:\d{2}(?::\d{2})?\b")
_NUM_PAT = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _values_from_rows(rows: list) -> list[float]:
    """All numeric values in a result set, plus per-column sums and the row
    count — the universe of figures a grounded answer may quote."""
    vals: list[float] = []
    col_sums: dict[str, float] = {}
    for r in rows[:_RESULT_ROWS]:
        if not isinstance(r, dict):
            continue
        for k, v in r.items():
            if isinstance(v, bool) or v is None:
                continue
            try:
                f = float(str(v).replace(",", "")) if isinstance(v, str) else float(v)
            except (ValueError, TypeError):
                continue
            vals.append(f)
            col_sums[k] = col_sums.get(k, 0.0) + f
    vals.extend(col_sums.values())
    vals.append(float(len(rows)))
    return vals


def _answer_numbers(text: str) -> list[tuple[float, bool, str]]:
    """(value, is_percentage, original_token) for every number in the answer,
    ignoring dates/times."""
    t = _DATE_TIME_PAT.sub(" ", text)
    out = []
    for m in _NUM_PAT.finditer(t):
        tok = m.group(0)
        try:
            n = float(tok.replace(",", ""))
        except ValueError:
            continue
        is_pct = t[m.end():m.end() + 1] == "%" or "percent" in t[m.end():m.end() + 9].lower()
        out.append((n, is_pct, tok))
    return out


def _is_grounded_number(n: float, allowed: list[float]) -> bool:
    if float(n).is_integer() and 0 <= n <= 100:      # ranks, counts, list positions
        return True
    if float(n).is_integer() and 1990 <= n <= 2100:  # years
        return True
    for a in allowed:
        # direct quote (allow rounding of big figures to ~0.05%)
        if abs(n - a) <= max(0.011, abs(a) * 0.0005):
            return True
        # "123.13 million" / "1.58 billion" style rescaling (~1%)
        for scale in (1e3, 1e6, 1e9):
            if abs(n * scale - a) <= abs(a) * 0.01:
                return True
    return False


_COUNT_Q_PAT = re.compile(r"(?i)\bhow many\b|\bnumber of\b|\bcount of\b")
_COUNT_COL_PAT = re.compile(
    r"(?i)count|cnt|total|num|qty|volume|^n$|^c$|merchants|stores|terminals|machines")


def _count_violation(question: str, answer: str, agg_values: list[float]) -> str:
    """For 'how many X' questions the headline count must come from an aggregate
    (COUNT-like column, small aggregate result, or an actual result row-count) —
    a value that merely collides with some ID in a lookup row does NOT count.
    The generic grounding check cannot catch that collision (a gateway_id of 107
    once 'grounded' a fabricated store count of 107)."""
    if not _COUNT_Q_PAT.search(question or ""):
        return ""
    t = _DATE_TIME_PAT.sub(" ", answer)
    cands = [b for b in re.findall(r"\*\*\s*([\d,]+)\s*\*\*", t)
             if b.replace(",", "").isdigit()]
    if not cands:
        m = _NUM_PAT.search(t)
        cands = [m.group(0)] if m else []
    for tok in cands:
        try:
            n = float(tok.replace(",", ""))
        except ValueError:
            continue
        if float(n).is_integer() and 1990 <= n <= 2100:   # years
            continue
        if not any(abs(n - a) <= max(0.011, abs(a) * 0.0005) for a in agg_values):
            return (f"the count {tok} was never computed by any aggregate query "
                    "(no COUNT(*)/SUM() result or row count matches it)")
    return ""


def _grounding_violations(answer: str, allowed: list[float], has_rows: bool) -> str:
    """Returns "" when the answer is grounded, else a description of what's wrong."""
    problems = []
    if _PLACEHOLDER_PAT.search(answer):
        problems.append("it contains placeholder/example content (fake names, 'example "
                        "table', 'actual results would be inserted') — that is NEVER acceptable")
    # Pairwise sums/differences of result values are legitimate derived figures
    # ("grew by Rs X") — allow them when the value set is small enough.
    derived = list(allowed)
    if 0 < len(allowed) <= 120:
        for i, a in enumerate(allowed):
            for b in allowed[i + 1:]:
                derived.append(a - b)
                derived.append(b - a)
                derived.append(a + b)
    bad = []
    for n, is_pct, tok in _answer_numbers(answer):
        if is_pct:
            continue
        if not _is_grounded_number(n, derived):
            bad.append(tok)
    if bad:
        uniq = list(dict.fromkeys(bad))[:8]
        if has_rows:
            problems.append(f"these numbers do NOT appear in any query result: {uniq}")
        else:
            problems.append(f"it states figures ({uniq}) but NO query returned any rows")
    return "; ".join(problems)


def _deterministic_summary(rows: list) -> str:
    """Fallback answer built purely in code from the result rows — used when the
    model's narrative repeatedly fails the grounding check. Names the top entries
    by the dominant numeric column; every figure is copied from the rows."""
    try:
        first = rows[0]
        cols = list(first.keys())
        label_col = next((c for c in cols
                          if not isinstance(first.get(c), (int, float))
                          and not str(first.get(c, "")).replace(",", "").replace(".", "").isdigit()),
                         cols[0])

        def _num(v):
            try:
                return float(str(v).replace(",", ""))
            except (ValueError, TypeError):
                return None

        num_cols = [c for c in cols if c != label_col and _num(first.get(c)) is not None]
        if not num_cols:
            return (f"I retrieved {len(rows)} row(s) — shown in the table below, every value "
                    "straight from the database. Ask me to break it down differently if needed.")
        main = max(num_cols, key=lambda c: sum(abs(_num(r.get(c)) or 0) for r in rows[:200]))
        ranked = sorted(rows, key=lambda r: _num(r.get(main)) or 0, reverse=True)[:3]
        pretty = main.replace("_", " ")
        tops = "; ".join(
            f"**{r.get(label_col)}** ({_num(r.get(main)) or 0:,.2f})" for r in ranked)
        return (f"The table below has the full result ({len(rows)} rows, every value straight "
                f"from the database). Top by {pretty}: {tops}. I couldn't produce a reliable "
                f"written analysis this time — ask me to break the result down differently "
                f"for more detail.")
    except Exception:
        return ("I retrieved the data shown in the results table below — every value there "
                "comes directly from the database. Ask me to break the result down "
                "differently if you'd like more detail.")


# =========================================================
# AGENT LOOP
# =========================================================
def _parse_action(raw: str) -> dict | None:
    txt = (raw or "").strip()
    txt = re.sub(r"^```(?:json)?\s*", "", txt)
    txt = re.sub(r"\s*```$", "", txt)
    try:
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else None
    except Exception:
        m = re.search(r"\{.*\}", txt, flags=re.S)
        if m:
            try:
                obj = json.loads(m.group(0))
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None
    return None


def _observation(rows) -> str:
    if isinstance(rows, dict) and "error" in rows:
        return (f"QUERY ERROR: {rows['error']}\n"
                "Fix the SQL (check table prefixes, column names, quoting) and try again.")
    if not isinstance(rows, list):
        return f"UNEXPECTED RESULT: {str(rows)[:500]}"
    n = len(rows)
    body = json.dumps(rows[:_OBS_ROWS], default=str, ensure_ascii=False)
    if len(body) > _OBS_CHARS:
        body = body[:_OBS_CHARS] + " ...(truncated)"
    header = f"RESULT: {n} row(s)"
    if n > _OBS_ROWS:
        header += f" (showing first {_OBS_ROWS})"
    if n == 0:
        header += (" — empty. Either the filter is wrong (spelling? join? date column?) "
                   "or there is genuinely no matching data; verify before concluding.")
    return header + "\n" + body


_VOICE_FIXES = [
    (re.compile(r"(?i)\bthe data (?:you|you've|you have) (?:shared|provided|given|sent)\b"),
     "our database"),
    (re.compile(r"(?i)\bthe (?:provided|shared|given) data\b"), "the database"),
    (re.compile(r"(?i)\byour data\b"), "our data"),
]


def _sanitize_voice(text: str) -> str:
    for pat, rep in _VOICE_FIXES:
        text = pat.sub(rep, text)
    return text


def _history_block(history) -> str:
    if not isinstance(history, list) or not history:
        return ""
    lines = []
    for m in history[-4:]:
        role = "User" if m.get("role") == "user" else "Assistant"
        content = str(m.get("content", ""))[:400]
        lines.append(f"{role}: {content}")
    return "Recent conversation (for follow-up context only):\n" + "\n".join(lines) + "\n\n"


def answer_with_agent(question: str, sql_executor, history=None) -> dict | None:
    """Returns a payload dict compatible with handle_user_question, or None so the
    caller can fall back to the legacy pipeline."""
    try:
        return _run_agent(question, sql_executor, history)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[agent_engine] agent failed: {e}")
        return None


def _with_query_cap(sql: str) -> str:
    """Inject a MAX_EXECUTION_TIME optimizer hint so agent queries die at ~45s
    instead of run_sql's 3-minute session cap (6 slow queries would otherwise
    stack up to a 15+ minute request). Only works on top-level SELECT; WITH
    queries keep the session cap."""
    return re.sub(r"(?i)^(\s*)SELECT\b",
                  rf"\1SELECT /*+ MAX_EXECUTION_TIME({_QUERY_TIMEOUT_MS}) */",
                  sql, count=1)


def _run_agent(question: str, sql_executor, history=None) -> dict | None:
    messages = [
        {"role": "system", "content": _build_system_prompt()},
        {"role": "user", "content": _history_block(history) + f"Question: {question}"},
    ]

    steps = []            # [{"sql", "row_count"|None, "error"|None}]
    last_sql = None
    last_rows = None      # last successful list result
    queries_used = 0
    consecutive_timeouts = 0
    grounding_retries = 0
    grounded_values = []  # every numeric value seen in any successful result
    agg_values = []       # values a stated COUNT may legitimately come from:
                          # row counts, small aggregate results, COUNT-like columns
    t0 = time.monotonic()

    parse_failures = 0
    for _turn in range(_MAX_TURNS):
        resp = _chat(
            model=_MODEL,
            temperature=0,
            max_tokens=5000,
            reasoning_effort="none",  # disable Gemini thinking; else it eats the token budget
            response_format={"type": "json_object"},
            messages=messages,
        )
        raw = resp.choices[0].message.content or ""
        act = _parse_action(raw)
        messages.append({"role": "assistant", "content": raw})

        if act is None:
            # A huge final table once overflowed max_tokens -> broken JSON on every
            # retry -> the whole (correct!) result was thrown away. After 2 failures,
            # stop retrying and salvage the rows below.
            parse_failures += 1
            if parse_failures >= 2 and isinstance(last_rows, list) and last_rows:
                break
            messages.append({"role": "user",
                             "content": "Reply with ONE valid JSON object exactly as instructed. "
                                        "Keep the answer SHORT — the result table is already "
                                        "shown to the user; summarize only the top entries."})
            continue
        parse_failures = 0

        if act.get("action") == "final" or (act.get("answer") and not act.get("sql")):
            answer = _sanitize_voice((act.get("answer") or "").strip())
            if not answer:
                return None
            # Hard grounding check: every figure in the answer must exist in the
            # query results. Prompt rules alone proved insufficient (the model
            # once emitted a fake "example" RM table with invented values).
            problems = _grounding_violations(answer, grounded_values,
                                             has_rows=bool(last_rows))
            count_problem = _count_violation(question, answer, agg_values)
            if count_problem:
                problems = f"{problems}; {count_problem}" if problems else count_problem
            if problems:
                print(f"[agent_engine] GROUNDING CHECK FAILED: {problems}")
                if grounding_retries < 2:
                    grounding_retries += 1
                    if count_problem and queries_used < _MAX_QUERIES:
                        messages.append({"role": "user", "content":
                            f"GROUNDING CHECK FAILED — your answer was NOT sent to the "
                            f"user because {count_problem}. Run ONE small COUNT(*) "
                            f"query NOW (action \"sql\" or \"mart_sql\") that computes "
                            f"the exact count the question asks for, then give your "
                            f"final answer quoting that result digit-for-digit."})
                        continue
                    messages.append({"role": "user", "content":
                        f"GROUNDING CHECK FAILED — your answer was NOT sent to the user "
                        f"because {problems}. Rewrite your final answer now (action "
                        f"\"final\") using ONLY values copied digit-for-digit from your "
                        f"query results, and describe each figure as what its column "
                        f"actually is (e.g. POS-only GMV is not 'combined GMV'). If no "
                        f"query returned the data needed, say plainly that you could not "
                        f"retrieve it — NEVER invent example or illustrative values."})
                    continue
                # Model kept fabricating → deterministic summary built straight
                # from the rows (grounded by construction), never a bare shrug.
                if isinstance(last_rows, list) and last_rows:
                    answer = _deterministic_summary(last_rows)
                else:
                    answer = ("I wasn't able to retrieve the data needed to answer this "
                              "— my queries returned no usable results, so I have no "
                              "figures to report. Try rephrasing the question or "
                              "narrowing the period.")
            return {
                "question": question,
                "sql": last_sql,
                "raw_result": last_rows[:_RESULT_ROWS] if isinstance(last_rows, list) else [],
                "answer": answer,
                "insights": answer,
                "response_type": "data_query",
                "engine": "agent",
                "agent_steps": steps,
            }

        sql = (act.get("sql") or "").strip()
        if not sql:
            messages.append({"role": "user",
                             "content": 'Your JSON had action "sql" but no "sql" text. Send it again.'})
            continue

        is_mart = act.get("action") == "mart_sql"

        elapsed = time.monotonic() - t0
        if (queries_used >= _MAX_QUERIES or elapsed > _MAX_SECONDS
                or consecutive_timeouts >= 3):
            messages.append({"role": "user",
                             "content": "STOP — no more queries (budget exhausted). Give your "
                                        'final grounded answer NOW (action "final") using only '
                                        "the results you already have; state clearly what you "
                                        "could not verify."})
            continue

        queries_used += 1
        src = "mart" if is_mart else "mysql"
        print(f"[agent_engine] query {queries_used}/{_MAX_QUERIES} ({src}):\n{sql[:300]}")
        if is_mart:
            from redesign.pos_summary_mart import mart_query
            rows = mart_query(sql)
        else:
            rows = sql_executor(_with_query_cap(sql))

        step = {"sql": sql, "source": src, "row_count": None, "error": None}
        if isinstance(rows, dict) and "error" in rows:
            step["error"] = rows["error"]
            _msg = str(rows["error"]).lower()
            if "took too long" in _msg or "execution time" in _msg:
                consecutive_timeouts += 1
            else:
                consecutive_timeouts = 0
        elif isinstance(rows, list):
            step["row_count"] = len(rows)
            last_sql = sql
            last_rows = rows
            consecutive_timeouts = 0
            grounded_values.extend(_values_from_rows(rows))
            agg_values.append(float(len(rows)))
            if len(rows) <= 3:
                agg_values.extend(_values_from_rows(rows))
            else:
                for _r in rows[:_RESULT_ROWS]:
                    for _k, _v in _r.items():
                        if _COUNT_COL_PAT.search(_k):
                            try:
                                agg_values.append(float(str(_v).replace(",", "")))
                            except (ValueError, TypeError):
                                pass
        steps.append(step)

        obs = _observation(rows)
        if queries_used >= _MAX_QUERIES or (time.monotonic() - t0) > _MAX_SECONDS:
            obs += ("\n\nThat was your LAST query (budget exhausted). Give your final answer "
                    'next (action "final").')
        elif consecutive_timeouts >= 2:
            obs += ("\n\nTwo timeouts in a row — do NOT try another heavy query. Either run one "
                    "drastically simpler/narrower query, or give your final answer with what "
                    "you have.")
        messages.append({"role": "user", "content": obs})

    # Turn/parse budget exhausted without a parseable final answer. If real rows came
    # back, SALVAGE them with a deterministic summary — the UI shows the table, and
    # returning None here once discarded a correct 240-row result and let a slow,
    # wrong-shaped legacy handler answer instead.
    if isinstance(last_rows, list) and last_rows:
        print("[agent_engine] budget exhausted — salvaging last result rows")
        answer = _deterministic_summary(last_rows)
        return {
            "question": question,
            "sql": last_sql,
            "raw_result": last_rows[:_RESULT_ROWS],
            "answer": answer,
            "insights": answer,
            "response_type": "data_query",
            "engine": "agent",
            "agent_steps": steps,
        }
    print("[agent_engine] turn budget exhausted without final answer")
    return None
