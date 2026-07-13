"""
Web research path — questions that explicitly ask to search the web/internet
("pick the top 10 and do a web search — should we onboard them again?") used to be
refused by the agent ("I do not have the capability to browse the internet").

Uses Gemini's native Google Search grounding (same GEMINI_API_KEY, no new deps —
httpx ships with the openai package). The OpenAI-compatible endpoint the rest of the
app uses does NOT expose search grounding, so this module calls the native
generateContent REST API directly.

Honesty contract:
  - Internal figures come ONLY from the conversation context (the previous data
    answers) — never invented.
  - External claims come from Google Search grounding and are answered with the
    sources listed, clearly separated from our data.
"""
from __future__ import annotations

import os
import re

import httpx
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

_KEY = os.getenv("GEMINI_API_KEY")
_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{_MODEL}:generateContent"

_WEB_PAT = re.compile(
    r"(?i)\bweb ?search\b|\bsearch (?:the )?(?:web|internet|net|online|google)\b|"
    r"\bgoogle (?:it|them|search)\b|\bsearch online\b|\bonline search\b|"
    r"\blook (?:it|them|this) up online\b|\bresearch (?:them |it )?online\b|"
    r"\bfrom the (?:web|internet)\b|\bcheck (?:the )?(?:web|internet|online)\b")

# Asking for the SOURCES of industry/market context is asking us to look them up —
# the bot used to answer "I do not have specific, named sources" even though web
# research can find the actual reports (CBSL payments bulletins, LankaClear stats...).
_SOURCE_PAT = re.compile(
    r"(?i)\b(?:specific |named |actual |real )?sources?\b.{0,50}\b(?:industry|market|"
    r"benchmark|data|figures|statistics|stats|numbers|report)"
    r"|\b(?:industry|market)\b.{0,40}\bsources?\b"
    r"|\bwhere (?:did|does|do) (?:you|that|this|the).{0,40}\b(?:industry|market|"
    r"benchmark).{0,20}(?:come from|from)")


# "get live market data and check", "fetch the latest industry figures", "verify
# against current market numbers" — fetch-verb + external-data noun = a web request,
# even without the words "web" or "search".
_EXTERNAL_PAT = re.compile(
    r"(?i)\b(?:get|fetch|find|pull|grab|check|look ?up|verify|confirm)\b"
    r".{0,50}\b(?:market|industry|competitor)s?\b"
    r"|\b(?:live|latest|real|actual|current|external|published|official|up[- ]?to[- ]?date)\b"
    r".{0,25}\b(?:market|industry|competitor)s?\b"
    r".{0,35}\b(?:data|figures|numbers|stats|statistics|growth|reports?|benchmarks?)\b")


# "how do we compare our first 6 months with the market growth?" — comparing OUR
# results against the MARKET needs real published data, so it belongs here (grounded
# search + our measured facts), not the advisor's general knowledge. NOTE: the
# PRIMARY router is the interpreter LLM's `source` classification (it now runs on
# every turn, including the first); all patterns in this file are only the outage
# fallback for when that LLM call fails. Internal comparisons ("compare june vs may",
# "compared to other merchants") don't mention market/industry/competitors and are
# untouched.
# NOTE "the market/the industry" (with article) on purpose: "industry-wise
# comparison of our merchants" is an INTERNAL MCC breakdown and must not come here.
_MARKET_COMPARE_PAT = re.compile(
    r"(?i)\b(?:compar\w*|benchmark\w*|stack up|measure up|keeping pace)\b"
    r".{0,80}\b(?:the market|the industry|competitors?)\b"
    r"|\bthe (?:market|industry)\b.{0,60}\b(?:compar\w*|benchmark\w*)\b"
    r"|\b(?:vs\.?|versus|against) the (?:market|industry|competition)\b")


# "what are the latest CBSL regulations on payment gateways" — regulator names and
# regulation nouns are PUBLIC information published by the regulator, never in our
# database, so they always belong to web research. Without this the question fell to
# the knowledge classifier, which refused it ("my capabilities are limited to querying
# the WEBXPAY internal databases"). Internal data questions (GMV, merchants, RMs...)
# never contain these words, so the pattern is safe to keep broad.
_REGULATORY_PAT = re.compile(
    r"(?i)\bcbsl\b|\bcentral bank\b|\blanka ?clear\b|\bpci[ -]?dss\b|"
    r"\bregulat(?:ion|ions|ory|or|ors)\b|"
    r"\b(?:directive|circular|gazette)s?\b|"
    r"\bcompliance (?:rule|requirement|standard|guideline)s?\b|"
    r"\bpayment (?:and settlement|systems?) act\b")


def is_web_question(question: str) -> bool:
    q = question or ""
    return bool(_WEB_PAT.search(q) or _SOURCE_PAT.search(q)
                or _EXTERNAL_PAT.search(q) or _MARKET_COMPARE_PAT.search(q)
                or _REGULATORY_PAT.search(q))


def _history_context(history) -> str:
    """Most recent turns, most weight on the last data answer (that's usually the
    list the user wants researched)."""
    if not isinstance(history, list) or not history:
        return "(no prior conversation)"
    lines = []
    for m in history[-6:]:
        role = "User" if m.get("role") == "user" else "Assistant"
        content = str(m.get("content", ""))[:2500]
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def _internal_facts(sql_executor) -> str:
    """Measured company facts (mart + one small MySQL query) so internal figures are
    the SAME regardless of chat history. Before this, web answers quoted whatever
    numbers happened to sit in the last few turns — the identical market-comparison
    question cited '119 merchants' in one chat and the full outlook figures in another."""
    if sql_executor is None:
        return ""
    try:
        import json as _json
        from outlook import _gather_facts
        facts = _gather_facts(sql_executor)
        if facts:
            return _json.dumps(facts, indent=2, default=str)
    except Exception as e:
        print(f"[web_research] internal facts unavailable: {e}")
    return ""


def handle_web_research(question: str, history=None, sql_executor=None) -> dict | None:
    """One grounded Gemini call with Google Search enabled. Returns a payload dict
    compatible with handle_user_question, or None on failure (caller falls through)."""
    if not _KEY:
        return None

    facts_block = _internal_facts(sql_executor)
    facts_section = (
        f"\nOUR MEASURED INTERNAL DATA (fresh from our database — the AUTHORITATIVE "
        f"source for any WEBXPAY figure; prefer these over numbers in the conversation "
        f"if they differ):\n{facts_block}\n" if facts_block else "")

    prompt = f"""You are the WEBXPAY analytics assistant (Sri Lankan payment gateway).
The user asked something needing WEB research — external market/industry facts, or
researching merchants from our own database shown earlier in the conversation.
{facts_section}
CONVERSATION SO FAR (quote internal figures ONLY from this or the measured data above,
never invent them):
{_history_context(history)}

USER REQUEST: {question}

Use Google Search to research the external facts (are these businesses still operating,
their reputation/news/online presence, anything relevant to the request). Then answer:
- Keep OUR internal figures (GMV, transaction counts, RM names) clearly separated from
  WEB FINDINGS.
- For each entity researched: 1-2 sentences on what the web shows + a practical verdict
  (e.g. worth a win-back call / appears closed / switched provider / unclear).
- If the web has little or nothing on an entity, say exactly that — never pad.
- Finish with a short prioritized recommendation list.
- Markdown, concise. Do not repeat the full internal table."""

    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 2500},
    }
    try:
        r = httpx.post(_URL, params={"key": _KEY}, json=body, timeout=120)
        r.raise_for_status()
        data = r.json()
        cand = (data.get("candidates") or [{}])[0]
        parts = (cand.get("content") or {}).get("parts") or []
        answer = "\n".join(p.get("text", "") for p in parts if p.get("text")).strip()
        if not answer:
            return None
        # Attach grounding sources so the user can verify the web claims.
        chunks = ((cand.get("groundingMetadata") or {}).get("groundingChunks") or [])
        seen, sources = set(), []
        for ch in chunks:
            web = ch.get("web") or {}
            title, uri = (web.get("title") or "").strip(), web.get("uri")
            if uri and title not in seen:
                seen.add(title)
                sources.append(f"- {title}")
        if sources:
            answer += "\n\n**Web sources consulted:** \n" + "\n".join(sources[:8])
    except Exception as e:
        print(f"[web_research] failed: {e}")
        return None

    return {
        "question": question,
        "sql": None,
        "raw_result": [],
        "answer": answer,
        "insights": answer,
        "response_type": "conversation",
        "engine": "web",
    }
