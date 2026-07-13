"""
New-engine entry point, shaped to drop into app.py's /ask exactly like the legacy
handle_user_question(question, sql_executor, history) -> payload dict.

Returns the SAME keys the legacy function returns (question, sql, raw_result, answer,
insights, response_type) so app.py's existing response-normalisation works unchanged.

It reuses the already-tested pipeline pieces from this folder:
  route/plan/build_sql (demo.py) -> validator -> executor -> answer.explain
"""
from __future__ import annotations
import os, sys

# parent dir (gpt_helpers, db) and this dir (siblings) both importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from validator import validate_sql, with_timeout_hint
from schema_loader import get_schema_tables, source as schema_source
from answer import explain
from demo import route, plan, build_sql
import gpt_helpers as G


def _payload(question, sql=None, raw_result=None, answer="", response_type="data_query"):
    return {
        "question": question,
        "sql": sql,
        "raw_result": raw_result if raw_result is not None else [],
        "answer": answer,
        "insights": answer,
        "response_type": response_type,
        "engine": "new",          # so you can confirm in the response which engine answered
    }


def handle_user_question_new(question: str, sql_executor, history=None):
    # 1) ROUTE — db / web / greeting (history-aware so follow-ups like
    #    "above is ipg or pos" / "what about POS?" resolve into standalone questions)
    try:
        r = route(question, history=history)
    except Exception:
        r = {"lane": "db", "standalone_question": question}
    q = r.get("standalone_question") or question
    lane = r.get("lane", "db")

    if lane == "chit_chat":
        # Greetings AND off-topic small talk ("what sound does a cat make") land here.
        # Answer warmly in one line, then redirect — never a robotic refusal.
        try:
            resp = G.client.chat.completions.create(
                model=G.CHAT_MODEL,
                messages=[{"role": "user", "content": (
                    "You are the WEBXPAY analytics assistant (Sri Lankan payment "
                    "gateway). The user sent a greeting or an off-topic casual "
                    "message. Reply in ONE short, warm sentence that genuinely "
                    "answers it (e.g. 'Meow! 🐱' for a cat-sound question), then ONE "
                    "sentence steering back to what you do: GMV, revenue, merchants, "
                    "POS/IPG analytics. Never refuse robotically.\n\n"
                    f"User message: {q}")}],
                temperature=0.4,
                max_tokens=120,
                reasoning_effort="none",
            )
            txt = (resp.choices[0].message.content or "").strip()
            if txt:
                return _payload(q, answer=txt, response_type="greeting")
        except Exception as _e:
            print(f"[engine] chit_chat reply failed, using canned greeting: {_e}")
        return _payload(q, answer=(
            "Hello! I'm the WEBXPAY Analytics Assistant (new engine). Ask me about GMV, "
            "revenue, MDR, merchants, POS or IPG — e.g. *'IPG revenue for March 2025'*."
        ), response_type="greeting")

    if lane in ("web", "hybrid"):
        # Grounded Google-Search research — same module the legacy engine uses, so
        # "latest CBSL regulations" etc. get a real sourced answer, not a stub.
        try:
            from web_research import handle_web_research
            _web = handle_web_research(q, history=history, sql_executor=sql_executor)
            if isinstance(_web, dict) and _web.get("answer"):
                _web["engine"] = "new"
                return _web
        except Exception as _e:
            print(f"[engine] web research failed: {_e}")
        return _payload(q, answer=(
            f"**This looks like a public/external question** ({r.get('reason','')}), "
            "but the web-research service couldn't be reached just now — please try again."
        ), response_type="web")

    # Month-over-month merchant mover questions have a deterministic multi-query handler
    # that returns one row per merchant with combined/IPG/POS columns and RM name. Route
    # these before the planner can send complex wording to adhoc SQL.
    ql = q.lower()
    _two_months = G._parse_two_months(q)
    if _two_months:
        _mover_words = (
            "drop", "dropped", "decline", "declined", "fell", "fall", "decrease",
            "decreased", "reduction", "improved", "improve", "increase", "increased",
            "compared", "compare", "vs", "versus",
        )
        _merchant_words = ("merchant", "store", "rm", "rmm", "relationship manager")
        _channel_words = ("combined", "ipg", "pos", "separate", "separately")
        if (any(w in ql for w in _mover_words)
                and any(w in ql for w in _merchant_words)
                and (any(w in ql for w in _channel_words) or "mcc" in ql)):
            out = G.handle_month_gmv_comparison(q, sql_executor, months=_two_months)
            if isinstance(out, dict):
                out.setdefault("response_type", "detail_rows")
                out["engine"] = "new"
                return out

    # 2) PLAN
    pl = plan(q)
    tool = pl.get("tool") if isinstance(pl, dict) else None

    # 2a-fast-gmv) POS per-merchant GMV thresholds/rankings via the LOCAL mart.
    #     "POS merchants with less than 350k last month", "POS merchants above 1m", etc.
    #     Live computation needs the heavy dedup, so serve it from the pre-aggregated mart.
    if "pos" in q.lower():
        try:
            from pos_summary_mart import _parse_threshold, summary_ready, handle_pos_merchant_gmv
            if (_parse_threshold(q) is not None
                    and any(w in q.lower() for w in ("merchant", "gmv", "transaction", "sales", "turnover"))
                    and summary_ready()):
                p = pl.get("params", {}) if isinstance(pl, dict) else {}
                out = handle_pos_merchant_gmv(q, p.get("date_start"), p.get("date_end"), sql_executor)
                if isinstance(out, dict):
                    return out
        except Exception as _e:
            print(f"[engine] pos gmv mart path unavailable, using normal path: {_e}")

    # 2a-fast) POS non-transacting via the LOCAL pre-aggregated mart (read-only-DB safe).
    #     Combines the small MySQL merchant list with local SQLite activity in Python —
    #     avoids the 30s+ dedup GROUP BY. Falls through to the heavy query if not built.
    if tool == "non_transacting_merchants" and (("pos" in q.lower()) or ("ipg" not in q.lower())):
        try:
            from pos_summary_mart import summary_ready, handle_pos_non_transacting
            if summary_ready():
                p = pl.get("params", {}) if isinstance(pl, dict) else {}
                out = handle_pos_non_transacting(q, p.get("date_start"), p.get("date_end"), sql_executor)
                if isinstance(out, dict):
                    out.setdefault("response_type", "detail_rows")
                    return out
        except Exception as _e:
            print(f"[engine] pos mart path unavailable, using heavy query: {_e}")

    # 2a) FULL HANDLERS — these validated legacy handlers return a COMPLETE payload
    #     (they run multiple queries + ground the answer themselves). Use them directly.
    _FULL = {
        "gmv_drop_diagnosis": lambda: G.handle_gmv_drop_diagnosis(q, sql_executor),
        "merchant_movers":    lambda: G.handle_month_gmv_comparison(q, sql_executor),
    }
    if tool in _FULL:
        try:
            out = _FULL[tool]()
            if isinstance(out, dict):
                out.setdefault("response_type", pl.get("requested_output", "data_query"))
                out["engine"] = "new"
                return out
        except Exception as _e:
            print(f"[engine] full handler {tool} failed, falling through: {_e}")
        # handler returned None/failed -> continue to normal SQL path below

    # 3) BUILD SQL (reuses your validated builders)
    sql = build_sql(q, pl)

    # 4) VALIDATE
    ok, safe_sql, problems = validate_sql(
        sql, get_schema_tables(), max_rows=pl.get("params", {}).get("limit", 1000)
    )
    if not ok:
        return _payload(q, sql=sql,
                        answer=f"I couldn't build a safe query for that yet ({problems}). "
                               f"[schema source: {schema_source()}]",
                        response_type="error")
    safe_sql = with_timeout_hint(safe_sql)

    # 5) EXECUTE
    rows = sql_executor(safe_sql)
    if isinstance(rows, dict) and "error" in rows:
        return _payload(q, sql=safe_sql,
                        answer=f"**Database error:** {rows['error']}", response_type="error")

    # 6) ANSWER (grounded plain-English)
    try:
        text = explain(q, rows, pl)
    except Exception:
        text = "Query ran, but I couldn't generate the explanation."

    out = _payload(q, sql=safe_sql, raw_result=rows, answer=text,
                   response_type=pl.get("requested_output", "data_query"))
    # expose the plan so you can inspect routing/date decisions in the network tab
    out["plan"] = pl
    return out
