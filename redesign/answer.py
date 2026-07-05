"""
ANSWER step — turns query result rows into a plain-English business explanation.

Grounding rules (enforced by the prompt AND a post-check):
- Only uses numbers present in the rows / computed facts. Never invents figures.
- Matches the shape of the answer to what was asked (summary / comparison / trend / ...).
- States the exact date range, channel, and any assumptions.
- Handles empty results honestly.

Self-test:  python redesign/answer.py     (uses mock rows, no database needed)
"""
from __future__ import annotations
import os, sys, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import gpt_helpers as G   # reuse the Gemini client + _compute_result_facts grounding


ANSWER_SYS = """You are the WEBXPAY Analytics Assistant explaining a result to a
non-technical business user at a Sri Lankan payment gateway.

GROUNDING (non-negotiable):
- Use ONLY the numbers in RESULT_ROWS and RESULT_FACTS. If a number is not there, say it
  isn't in the returned data. Never estimate, round from memory, or invent a value.
- Never say "the data you shared" / "your data" — the data comes from the WEBXPAY database.
- If RESULT_ROWS is empty, clearly say no matching records were found for the exact filters
  and period, and state the period/channel used.

MATCH THE REQUESTED OUTPUT (given below):
- summary        -> the headline number(s); no raw dump.
- detail_rows    -> present the actual records; do not collapse to a summary.
- comparison     -> compare the exact periods/channels; show both values, the delta, and the
                    % change; state the direction (up/down). If the base is 0 or missing,
                    say "n/a (no prior value)" — never divide by zero.
- trend          -> describe the movement over time; name the peak and trough.
- ranking        -> list the items in order with their values.
- explanation    -> explain the business reason using the per-item numbers.
- recommendation -> give 2-4 prioritised actions, each tied to a number in the data.

ALWAYS, briefly: state the exact date range used, the channel (IPG / POS / combined), and any
assumptions passed to you. Currency is LKR unless stated. Keep it plain-language first,
numbers second. End with 2-3 relevant follow-up questions.
"""


def explain(question: str, rows, plan: dict | None = None) -> str:
    plan = plan or {}
    facts = G._compute_result_facts(rows) if isinstance(rows, list) else ""
    params = plan.get("params", {})
    period = f"{params.get('date_start','?')} to {params.get('date_end','?')}"

    # Truncate rows sent to the model, but facts are computed over the FULL set above.
    sample = rows[:50] if isinstance(rows, list) else rows

    user = f"""QUESTION: {question}

requested_output: {plan.get('requested_output', 'summary')}
channel: {plan.get('channel', 'both')}
period_used: {period}
assumptions: {plan.get('assumptions') or 'none'}

RESULT_FACTS (computed over the FULL result set — authoritative for totals / max / min):
{facts or 'none'}

RESULT_ROWS (sample, may be truncated to 50):
{json.dumps(sample, indent=2, default=str)}
"""

    resp = G.client.chat.completions.create(
        model=G.CHAT_MODEL,
        messages=[{"role": "system", "content": ANSWER_SYS},
                  {"role": "user", "content": user}],
        temperature=0.2,
    )
    text = (resp.choices[0].message.content or "").strip()
    return _guard(text, rows, question)


def _guard(text: str, rows, question: str) -> str:
    """
    Deterministic safety net (belt-and-suspenders with the prompt):
    - empty result must NOT be dressed up as a real number.
    - strip the banned 'your data' phrasing if the model slips.
    """
    if isinstance(rows, list) and len(rows) == 0:
        if not any(w in text.lower() for w in ("no ", "none", "no matching", "0 ", "zero", "not find")):
            return ("No matching records were found for the exact period and filters requested. "
                    "Nothing was returned from the WEBXPAY database, so there is no figure to report.\n\n"
                    + text)
    for bad, good in (("the data you shared", "the WEBXPAY data"),
                      ("your data", "the WEBXPAY data"),
                      ("the data you provided", "the WEBXPAY data")):
        text = text.replace(bad, good).replace(bad.capitalize(), good.capitalize())
    return text


# ─────────────────────────────────────────────────────────────────────────
# Self-test with MOCK rows (no DB needed) — proves the plain-English output.
# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 70, "\nTEST 1 — comparison (POS vs IPG, May vs June)\n" + "=" * 70)
    plan1 = {"requested_output": "comparison", "channel": "both",
             "params": {"date_start": "2025-05-01", "date_end": "2025-07-01"},
             "assumptions": ["channel not specified individually -> showing both"]}
    rows1 = [
        {"month": "2025-05", "ipg_revenue_lkr": 4_820_000, "pos_revenue_lkr": 2_310_000},
        {"month": "2025-06", "ipg_revenue_lkr": 4_195_000, "pos_revenue_lkr": 2_640_000},
    ]
    print(explain("Compare POS and IPG revenue for May and June 2025", rows1, plan1))

    print("\n" + "=" * 70, "\nTEST 2 — empty result (honesty guard)\n" + "=" * 70)
    plan2 = {"requested_output": "summary", "channel": "ipg",
             "params": {"date_start": "2027-01-01", "date_end": "2027-02-01"}}
    print(explain("What was IPG revenue in January 2027?", [], plan2))
