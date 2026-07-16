"""
New orchestration flow (skeleton) — replaces the ~440-line keyword chain in
gpt_helpers.handle_user_question.

Pipeline:  route -> plan -> build/generate SQL -> validate -> execute -> ground -> answer

This file is a scaffold showing the shape. It reuses your EXISTING, validated builders
(the canonical SQL functions in gpt_helpers.py) as "tools" instead of deleting them.
Wire the real Gemini calls where marked TODO. Nothing here is executed yet.
"""
from __future__ import annotations
import json, time, logging

from db import run_sql
from validator import validate_sql, with_timeout_hint

# Reuse the crown-jewel builders as TOOLS (do not rewrite these):
import gpt_helpers as G

log = logging.getLogger("chatbot")


# --------------------------------------------------------------------------
# TOOL REGISTRY: planner tool name -> callable(params) -> sql (or dict of sqls)
# Each wraps an EXISTING builder so validated business logic is preserved.
# --------------------------------------------------------------------------
def _pos_summary(p):      return G.build_pos_sql({**p, "channel": "pos"})
def _ipg_gmv(p):          return G.build_gmv_sql(p["_q"], G.analyze_intent(p["_q"]))
def _timeseries(p):
    ch = p["channel"]
    if ch == "pos":  return G.build_pos_timeseries_sql(p["date_start"], p["date_end"], p["grain"])
    if ch == "ipg":  return G.build_ipg_timeseries_sql(p["date_start"], p["date_end"], p["grain"])
    return {"ipg": G.build_ipg_timeseries_sql(p["date_start"], p["date_end"], p["grain"]),
            "pos": G.build_pos_timeseries_sql(p["date_start"], p["date_end"], p["grain"])}
def _txn_status(p):       return G.build_txn_status_sql(p["_q"], p["date_start"], p["date_end"])
def _top_merchants(p):    return None   # handled by G.handle_top_merchants (full handler)
def _adhoc(p):            return G.generate_sql(p["_q"], G.load_schema())

TOOL_REGISTRY = {
    "pos_summary": _pos_summary,
    "ipg_summary": _ipg_gmv,
    "combined_summary": None,          # -> G.handle_combined_query (full handler)
    "timeseries": _timeseries,
    "txn_status": _txn_status,
    "top_merchants": _top_merchants,   # -> G.handle_top_merchants
    "merchant_onboarding": lambda p: G.build_merchant_onboarding_sql(p["_q"], p.get("date_start"), p.get("date_end")),
    "gmv_drop_diagnosis": None,        # -> G.handle_gmv_drop_diagnosis
    "adhoc_sql": _adhoc,
}

# Some questions need a MULTI-STEP handler, not a single SQL. Map those tools to the
# existing high-level handlers, which already return a finished payload.
FULL_HANDLERS = {
    "combined_summary": lambda q, ex: G.handle_combined_query(q, G.analyze_intent(q), ex),
    "top_merchants":    lambda q, ex: G.handle_top_merchants(q, G.analyze_intent(q), ex),
    "gmv_drop_diagnosis": lambda q, ex: G.handle_gmv_drop_diagnosis(q, ex),
    "period_comparison": lambda q, ex: G.handle_month_gmv_comparison(q, ex),
}

# Load schema once at import for the validator (table -> columns).
SCHEMA_TABLES = G.load_schema_tables() if hasattr(G, "load_schema_tables") else {}


# --------------------------------------------------------------------------
# LLM steps (wire to Gemini). Kept tiny; prompts live in redesign/prompts/.
# --------------------------------------------------------------------------
def route(question: str, history: list) -> dict:
    """Lane selection + follow-up rewrite. Prompt: 1_router_prompt.md"""
    # TODO: call Gemini with router prompt, JSON mode, temperature 0.
    ...

def plan(question: str) -> dict:
    """Structured plan (tool + params). Prompt: 2_planner_prompt.md"""
    # TODO: call Gemini with planner prompt, JSON mode, inject today's date.
    ...

def answer(question: str, rows, facts: str, plan_obj: dict) -> str:
    """Grounded explanation. Prompt: 4_answer_prompt.md"""
    # TODO: call Gemini with answer prompt. Rows + facts only.
    ...

def web_answer(question: str) -> dict:
    """Web lane: search + cite. Uses a web-search tool; never touches the DB."""
    ...


# --------------------------------------------------------------------------
# MAIN ENTRY (drop-in replacement for handle_user_question)
# --------------------------------------------------------------------------
def handle_user_question(question: str, sql_executor=run_sql, history=None) -> dict:
    t0 = time.time()
    history = history or []

    # 1) ROUTE ----------------------------------------------------------------
    r = route(question, history)
    q = r.get("standalone_question") or question
    lane = r.get("lane", "db")

    if r.get("needs_clarification"):
        return _payload(q, answer=r["clarifying_question"], response_type="clarify")
    if lane == "meta":
        return _payload(q, answer=r.get("reason") or "Let me clarify from the previous answer.",
                        response_type="conversation")
    if lane == "chit_chat":
        return _payload(q, answer="Hello! Ask me about GMV, revenue, merchants, POS or IPG.",
                        response_type="greeting")
    if lane == "web":
        w = web_answer(q)
        return _payload(q, answer=w["answer"], response_type="web", extra={"sources": w["sources"]})
    # hybrid: run db branch, then web branch, then merge (omitted for brevity)

    # 2) PLAN -----------------------------------------------------------------
    pl = plan(q)
    tool = pl.get("tool", "adhoc_sql")
    pl.setdefault("params", {})["_q"] = q

    # 2a) Multi-step handlers already return a finished payload
    if tool in FULL_HANDLERS:
        out = FULL_HANDLERS[tool](q, sql_executor)
        return _finish(out, pl, t0)

    # 3) BUILD or GENERATE SQL ------------------------------------------------
    builder = TOOL_REGISTRY.get(tool) or TOOL_REGISTRY["adhoc_sql"]
    sql = builder(pl["params"])

    # 4) VALIDATE -------------------------------------------------------------
    ok, sql, problems = validate_sql(sql, SCHEMA_TABLES, max_rows=pl["params"].get("limit", 1000))
    if not ok:
        log.warning("sql rejected: %s | %s", problems, sql)
        return _payload(q, answer=f"I couldn't build a safe query: {problems}", sql=sql,
                        response_type="error")
    sql = with_timeout_hint(sql)

    # 5) EXECUTE --------------------------------------------------------------
    rows = sql_executor(sql)
    if isinstance(rows, dict) and "error" in rows:
        return _payload(q, sql=sql, answer=f"**Database error:** {rows['error']}",
                        response_type="error")

    # 6) GROUND + ANSWER ------------------------------------------------------
    facts = G._compute_result_facts(rows) if isinstance(rows, list) else ""
    text = answer(q, rows, facts, pl)

    _log_turn(q, sql, rows, time.time() - t0, problems)   # observability
    return _payload(q, sql=sql, raw_result=rows, answer=text,
                    response_type=pl.get("requested_output", "data_query"),
                    extra={"assumptions": pl.get("assumptions"), "plan": pl})


# --------------------------------------------------------------------------
def _payload(question, sql=None, raw_result=None, answer="", response_type="data_query", extra=None):
    p = {"question": question, "sql": sql, "raw_result": raw_result if raw_result is not None else [],
         "answer": answer, "insights": answer, "response_type": response_type}
    if extra: p.update(extra)
    return p

def _finish(out, pl, t0):
    if isinstance(out, dict):
        out.setdefault("response_type", pl.get("requested_output", "data_query"))
    return out

def _log_turn(q, sql, rows, secs, problems):
    log.info(json.dumps({"q": q, "sql": sql, "rows": len(rows) if isinstance(rows, list) else None,
                         "secs": round(secs, 3), "warnings": problems}, default=str))
