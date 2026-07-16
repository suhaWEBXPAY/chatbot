"""
Advisor path — opinion / comparison / strategy questions ("is our GMV run good
compared to other fintechs", "what do you think of...", "how can we improve X")
get a real analyst-style answer instead of a refusal.

Honesty contract:
  - OUR figures come from measured internal facts (outlook._gather_facts) — the
    only WEBXPAY numbers the model may state.
  - Industry/competitor context comes from the LLM's general knowledge, and must
    be explicitly labeled as such (qualitative or well-known public magnitudes,
    never invented precise competitor figures).

Used by gpt_helpers.handle_user_question (after the outlook check, before the
router — grounded-DB-only prompts were refusing these questions).
"""
from __future__ import annotations

import json
import os
import re

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

_client = OpenAI(
    api_key=os.getenv("GEMINI_API_KEY"),
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
)
_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# NOTE: comparisons only count as "advisor" when they are against EXTERNAL entities
# (companies/fintechs/the market). "compared to other days/months/merchants/RMs" is a
# DATA question — an early version of this regex hijacked exactly that.
_ADVISOR_PAT = re.compile(
    r"(?i)\bdo you think\b|\bwhat do you think\b|\byour (?:opinion|view|take|assessment)\b|"
    r"\bin your opinion\b|"
    r"\bcompared? (?:to|with|against) (?:other|the) (?:fintech|compan|competitor|player|psp|provider|industry|market)|"
    r"\bother (?:fintech|payment|psp) compan|"
    r"\bindustry (?:standard|average|benchmark|norm)|\bbenchmark|\bcompetitors?\b|"
    r"\breal (?:world|market) comparison\b|\bworld market\b|"
    r"\bis (?:this|that|our|it) (?:good|bad|healthy|normal|strong|weak|okay|ok)\b(?!.{0,25}\bcompared to other (?:day|week|month|year|merchant|rm)s?\b)|"
    r"\badvice\b|\badvise\b|\brecommend|"
    r"\bsuggest(?:ion)?s?\b|\bhow (?:can|do|should) we improve\b|\bstrategy\b|\bstrategic\b|"
    r"\b(?:market|industry|current|ongoing) trends?\b|"
    # "compare our results with the market growth" — normally web_research takes this
    # first (real published data); this is the fallback when the web path fails, so
    # the question never drops to the generic knowledge classifier's canned advice.
    r"\bcompar\w*\b.{0,80}\b(?:market|industry|competitors?)\b|"
    r"\b(?:market|industry) growth\b|"
    r"\bare we\b.{0,50}\b(?:up to date|up-to-date|competitive|on track|keeping up|"
    r"behind|ahead|doing (?:good|well|ok|okay))\b|"
    r"\bhow (?:are|is) (?:we|the company|webxpay) (?:doing|performing)\b")


# Questions that ask to ANALYZE OUR OWN transaction data (decline/abandonment
# rates, gateway/channel performance...) need real numbers computed first — the
# agent does that and can advise on top. The advisor would answer generically
# without ever querying, which is worse.
_DATA_ANALYSIS_PAT = re.compile(
    r"(?i)\banaly[sz]e\b.{0,40}\b(?:data|transaction|gmv|sales)|"
    r"\bdeclin\w*\b|\babandon\w*\b|\bchurn\b|\bgateway.wise\b|\bmerchant.wise\b|\brm.wise\b")


def is_advisor_question(question: str) -> bool:
    q = question or ""
    if _DATA_ANALYSIS_PAT.search(q):
        return False
    return bool(_ADVISOR_PAT.search(q))


_SYSTEM = """You are the WEBXPAY analytics advisor — a sharp fintech analyst for a Sri Lankan
payment gateway (IPG = online gateway, POS = card machines). The user wants your
JUDGMENT, not just numbers. Give a genuine, useful assessment — do not refuse and do
not hedge into uselessness.

You have TWO sources, and you must keep them clearly separated in your wording:
1. OUR MEASURED DATA (provided below) — the ONLY WEBXPAY figures you may state.
   Quote them exactly; never invent internal numbers beyond these.
2. YOUR GENERAL INDUSTRY KNOWLEDGE — payments industry norms, how PSPs/gateways are
   typically judged (take rate, GMV growth, merchant growth, channel mix), regional
   context for South Asian / Sri Lankan fintech, and well-known public facts about the
   space. Frame these as general knowledge ("typically", "industry-wide", "as a rough
   benchmark"), NEVER as live data. Do NOT state precise current figures for named
   competitors — use magnitudes and qualitative comparisons instead.

Structure: lead with your verdict in one sentence, support it with our measured figures
vs the relevant benchmarks/norms, note what would strengthen or threaten the position,
and close with 1-2 concrete things worth watching or doing. 3-4 short paragraphs,
markdown, **bold** the key figures and the verdict. End with one line noting the
industry context is general knowledge, not a live market feed."""


def handle_advisor(question: str, sql_executor, history=None) -> dict | None:
    try:
        from outlook import _gather_facts
        facts = _gather_facts(sql_executor) or {}
    except Exception:
        facts = {}

    ctx = ""
    if isinstance(history, list) and history:
        turns = [f"{m.get('role','user')}: {str(m.get('content',''))[:300]}" for m in history[-4:]]
        ctx = "Recent conversation:\n" + "\n".join(turns) + "\n\n"

    facts_block = (json.dumps(facts, indent=2, default=str) if facts
                   else "(internal metrics unavailable right now — say so and answer "
                        "qualitatively from general knowledge)")
    try:
        resp = _client.chat.completions.create(
            model=_MODEL,
            temperature=0.4,
            max_tokens=1600,
            reasoning_effort="none",  # disable Gemini thinking; else it eats the token budget
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": f"{ctx}OUR MEASURED DATA:\n{facts_block}\n\n"
                                            f"Question: {question}"},
            ],
        )
        answer = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        print(f"[advisor] failed: {e}")
        return None
    if not answer:
        return None

    return {
        "question": question,
        "sql": None,
        "raw_result": [],
        "answer": answer,
        "insights": answer,
        "response_type": "conversation",
        "engine": "advisor",
    }
