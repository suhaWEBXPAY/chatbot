"""
Forward-looking / speculative questions ("where would webxpay be in 10 years",
"forecast next year's GMV") — answered as GROUNDED PROJECTIONS instead of refused.

How it stays honest (the no-fabrication rule still applies):
  1. Real history is read from the local summary mart (monthly GMV both channels)
     and one small MySQL query (merchants onboarded per year).
  2. Growth rates and scenario projections are computed IN CODE (deterministic),
     never by the LLM.
  3. The LLM only narrates, using exclusively the computed figures, and the answer
     is explicitly labeled as a projection with its assumptions.

Used by gpt_helpers.handle_user_question (before question classification, which
previously refused these questions outright).
"""
from __future__ import annotations

import os
import re
from datetime import date

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

_client = OpenAI(
    api_key=os.getenv("GEMINI_API_KEY"),
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
)
_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

_HORIZON_PAT = re.compile(r"(?i)\b(?:in|after|next|coming|within)\s+(\d{1,2})\s+(year|yr)s?\b")
_FUTURE_PAT = re.compile(
    r"(?i)\bforecast\b|\bpredict|\bprojection|\bproject\b|\boutlook\b|"
    r"\bwhere (?:would|will|could|do you see)\b|\bfuture of\b|\bin the future\b|"
    r"\bnext year\b.{0,30}\b(?:look|be|reach)|\b(?:will|would|could)\b.{0,40}\b(?:be|reach|grow|look like)\b.{0,30}\b(?:year|future|20\d\d)\b")


# SHORT-HORIZON projections ("GMV projection for TODAY", "this week", "end of day",
# "this month") are RUN-RATE questions, not multi-year outlooks — the agent computes
# them from actuals + historical pace. This module once hijacked "projection for
# today" and returned the canned 2026-2031 scenario table three times in a row.
_SHORT_HORIZON_PAT = re.compile(
    r"(?i)\btoday\b|\btonight\b|\bend of (?:the )?day\b|\beod\b|\btomorrow\b|"
    r"\bthis (?:week|month)\b|\bby (?:month|week)[- ]end\b|\brest of (?:the )?(?:day|week|month)\b")


def is_outlook_question(question: str) -> bool:
    q = question or ""
    if _SHORT_HORIZON_PAT.search(q):
        return False
    if _HORIZON_PAT.search(q):
        return True
    return bool(_FUTURE_PAT.search(q))


def _horizon_years(question: str) -> int:
    m = _HORIZON_PAT.search(question or "")
    if m:
        return max(1, min(int(m.group(1)), 25))
    return 10 if "future" in (question or "").lower() else 5


# =========================================================
# MEASURED HISTORY (mart + one small MySQL query)
# =========================================================
def _gather_facts(sql_executor) -> dict | None:
    try:
        from redesign.pos_summary_mart import pos_monthly_gmv, ipg_monthly_gmv, mart_coverage
        cov = mart_coverage()
        if not cov:
            return None
        today = date.today()
        pos = pos_monthly_gmv("2000-01-01", today.isoformat())          # {ym: gmv}
        ipg = {ym: v[0] for ym, v in ipg_monthly_gmv("2000-01-01", today.isoformat()).items()}
    except Exception:
        return None

    facts: dict = {"today": today.isoformat()}

    # POS year-over-year growth on matching complete months (needs both years).
    def _ym(y, m):
        return f"{y:04d}-{m:02d}"

    pairs = []
    for ym, cur in pos.items():
        y, m = int(ym[:4]), int(ym[5:7])
        prev = pos.get(_ym(y - 1, m))
        # only complete months (exclude current month)
        if prev and cur and ym < today.strftime("%Y-%m"):
            pairs.append((ym, cur, prev))
    pairs.sort()
    recent = pairs[-6:]  # most recent matched year — early ramp-up months distort YoY
    if len(recent) >= 3:
        cur_sum = sum(p[1] for p in recent)
        prev_sum = sum(p[2] for p in recent)
        if prev_sum > 0:
            facts["pos_yoy_growth_pct"] = round((cur_sum / prev_sum - 1) * 100, 1)
            facts["pos_yoy_months_compared"] = len(recent)

    # Recent momentum: last 6 complete months vs the 6 before, both channels.
    # A business that ramped hard then plateaued shows huge YoY but flat momentum —
    # momentum is the honest base for projecting forward.
    complete = [ym for ym in sorted(set(pos) | set(ipg)) if ym < today.strftime("%Y-%m")]
    if len(complete) >= 12:
        last6, prev6 = complete[-6:], complete[-12:-6]
        s_last = sum(pos.get(m, 0) + ipg.get(m, 0) for m in last6)
        s_prev = sum(pos.get(m, 0) + ipg.get(m, 0) for m in prev6)
        if s_prev > 0:
            hoh = s_last / s_prev - 1
            facts["recent_6mo_vs_prior_6mo_pct"] = round(hoh * 100, 1)
            facts["recent_momentum_annualized_pct"] = round(((1 + hoh) ** 2 - 1) * 100, 1)

    # Within-current-year trend: is GMV still growing month-to-month, or flat?
    # (A business can show huge YoY from last year's ramp-up while already plateauing.)
    ytd = [ym for ym in complete if ym.startswith(str(today.year))]
    if len(ytd) >= 4:
        first_v = pos.get(ytd[0], 0) + ipg.get(ytd[0], 0)
        last_v = pos.get(ytd[-1], 0) + ipg.get(ytd[-1], 0)
        if first_v > 0:
            months_span = len(ytd) - 1
            monthly = (last_v / first_v) ** (1 / months_span) - 1
            facts["within_year_trend_annualized_pct"] = round(((1 + monthly) ** 12 - 1) * 100, 1)
            facts["within_year_first_month_gmv"] = round(first_v, 2)
            facts["within_year_last_month_gmv"] = round(last_v, 2)

    # 2026 run rate: complete months of the current year, both channels.
    ytd_months = [ym for ym in sorted(set(pos) | set(ipg))
                  if ym.startswith(str(today.year)) and ym < today.strftime("%Y-%m")]
    if ytd_months:
        monthly = [pos.get(ym, 0) + ipg.get(ym, 0) for ym in ytd_months]
        facts["avg_monthly_combined_gmv"] = round(sum(monthly) / len(monthly), 2)
        facts["annual_run_rate_gmv"] = round(sum(monthly) / len(monthly) * 12, 2)
        facts["run_rate_months_used"] = f"{ytd_months[0]}..{ytd_months[-1]}"

    # Merchant base: onboarding per year (small table, fast) + active count.
    # CANONICAL onboarding definition — must match gpt_helpers.build_merchant_onboarding_sql
    # (COALESCE(credit_review_approved_date, date_registered), free_trail=0, no is_active):
    # this figure gets quoted in outlook/advisor/web answers and used to contradict the
    # direct "how many merchants onboarded this year" answer (471 vs 119 in two chats).
    try:
        rows = sql_executor(
            "SELECT YEAR(COALESCE(credit_review_approved_date, date_registered)) AS y, "
            "COUNT(*) AS n FROM tbl_store "
            "WHERE COALESCE(credit_review_approved_date, date_registered) IS NOT NULL "
            "AND free_trail = 0 "
            "GROUP BY YEAR(COALESCE(credit_review_approved_date, date_registered)) ORDER BY y")
        if isinstance(rows, list):
            per_year = {int(r["y"]): int(r["n"]) for r in rows if r.get("y")}
            facts["merchants_onboarded_per_year"] = {
                y: per_year[y] for y in sorted(per_year) if y >= today.year - 5}
            # cumulative base growth over the last 3 full years
            years = [y for y in sorted(per_year) if y < today.year]
            cum = 0
            cum_by_year = {}
            for y in sorted(per_year):
                cum += per_year[y]
                cum_by_year[y] = cum
            g = []
            for y in years[-3:]:
                if cum_by_year.get(y - 1):
                    g.append(cum_by_year[y] / cum_by_year[y - 1] - 1)
            if g:
                facts["merchant_base_cagr_pct"] = round(sum(g) / len(g) * 100, 1)
    except Exception:
        pass
    try:
        from redesign.pos_summary_mart import mart_query
        r = mart_query("SELECT COUNT(*) AS n FROM store_dim WHERE is_active=1 AND free_trail=0")
        if isinstance(r, list) and r:
            facts["active_merchants_now"] = int(r[0]["n"])
    except Exception:
        pass

    return facts if facts.get("annual_run_rate_gmv") else None


def _project(facts: dict, years: int) -> list[dict]:
    """Deterministic scenario projections from the measured run rate + growth.
    Scenarios come from the SPREAD of the measured signals (YoY, 6-month momentum,
    within-year trend), each clamped to a sane band — a business that ramped hard
    but has since plateaued gets an honest 'flat' conservative case instead of
    compounding its ramp-up rate for a decade."""
    def _clamp(g, hi=40.0):
        return max(-20.0, min(g, hi)) / 100.0

    candidates = [g for g in (facts.get("pos_yoy_growth_pct"),
                              facts.get("recent_momentum_annualized_pct"),
                              facts.get("within_year_trend_annualized_pct")) if g is not None]
    if len(candidates) >= 2:
        candidates.sort()
        cons_g = _clamp(candidates[0])
        base_g = _clamp(candidates[len(candidates) // 2])
        opt_g = _clamp(candidates[-1], hi=60.0)
        facts["growth_basis"] = ("scenarios span the measured signals: within-year trend, "
                                 "6-month momentum and year-over-year, clamped to -20%..+60%")
    elif candidates:
        base_g = _clamp(candidates[0])
        cons_g, opt_g = base_g / 2, _clamp(candidates[0] * 1.5, hi=60.0)
        facts["growth_basis"] = "single measured growth signal"
    else:
        base_g = 0.10  # no measurable growth -> conservative default, stated in answer
        cons_g, opt_g = 0.05, 0.15
        facts["growth_assumed"] = True
    scenarios = {
        "conservative_gmv_lkr": cons_g,
        "base_gmv_lkr": base_g,
        "optimistic_gmv_lkr": opt_g,
    }
    facts["scenario_growth_pct"] = {k.replace("_gmv_lkr", ""): round(v * 100, 1)
                                    for k, v in scenarios.items()}
    start = facts["annual_run_rate_gmv"]
    this_year = int(facts["today"][:4])
    rows = []
    for i in range(0, years + 1):
        row = {"year": this_year + i}
        for col, g in scenarios.items():
            row[col] = round(start * ((1 + g) ** i), 0)
        rows.append(row)
    return rows


# =========================================================
# NARRATIVE
# =========================================================
_NARRATIVE_PROMPT = """You are the WEBXPAY analytics assistant (Sri Lankan payment gateway: IPG = online
gateway, POS = card machines). The user asked a forward-looking question. You are given
MEASURED FACTS from the company's own database and DETERMINISTIC SCENARIO PROJECTIONS
computed in code from those facts. These are the ONLY numbers you may use — do not invent
or recompute any figure.

Write an engaging but honest outlook (3-4 short paragraphs, markdown, **bold** the key
figures):
1. Current scale: annual GMV run rate, active merchants, measured growth.
2. What the scenarios imply for the horizon year (quote the conservative/base/optimistic
   end values and their growth assumptions).
3. Qualitative drivers you may mention WITHOUT numbers: digital-payment adoption in
   Sri Lanka, merchant onboarding momentum, both-channel (IPG+POS) presence.
4. Close with a clear caveat: this is a projection from historical growth, not a
   prediction — actual results depend on market conditions, competition and execution.

Format LKR with thousand separators or "Rs X.XX billion" style (billions = value/1e9,
rounded to 2 decimals — the projection table shows exact figures anyway)."""


def handle_outlook(question: str, sql_executor, history=None) -> dict | None:
    facts = _gather_facts(sql_executor)
    if not facts:
        return None
    years = _horizon_years(question)
    rows = _project(facts, years)

    import json as _json
    user_msg = (f"Question: {question}\n\nMEASURED FACTS:\n{_json.dumps(facts, indent=2)}\n\n"
                f"SCENARIO PROJECTIONS ({years}-year horizon, computed in code):\n"
                f"{_json.dumps(rows, indent=2)}")
    try:
        resp = _client.chat.completions.create(
            model=_MODEL,
            temperature=0.3,
            max_tokens=1500,
            reasoning_effort="none",  # disable Gemini thinking; else it eats the token budget
            messages=[
                {"role": "system", "content": _NARRATIVE_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        answer = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        print(f"[outlook] narrative failed: {e}")
        answer = ""
    if not answer:
        g = facts.get("scenario_growth_pct", {})
        answer = (f"Projection from measured data: current annual GMV run rate is "
                  f"Rs {facts['annual_run_rate_gmv']:,.0f}. The table shows where "
                  f"{years} years of compounding leads under conservative/base/optimistic "
                  f"growth ({g}). This is a projection from historical growth, not a prediction.")

    return {
        "question": question,
        "sql": {"history": "local summary_mart.sqlite3 (monthly GMV, both channels) "
                           "+ tbl_store onboarding counts",
                "projection": f"computed in code: run rate x growth scenarios over {years} years"},
        "raw_result": rows,
        "answer": answer,
        "insights": answer,
        "response_type": "data_query",
        "engine": "outlook",
    }
