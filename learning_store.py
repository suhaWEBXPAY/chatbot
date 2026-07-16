"""
Persistent learning store — durable rules the user teaches the bot in chat.

When the user corrects the bot ("for active merchants you should check
is_active = 1 and free_trail = 0"), the correction is extracted into a short
durable rule and saved to learned_lessons.json. Every future prompt (agent
engine + legacy SQL generation + insights) gets the saved rules injected, so
the same mistake is not repeated across sessions.

Used from:
  - gpt_helpers.handle_user_question -> detect_correction() at the very start
  - agent_engine._build_system_prompt / gpt_helpers prompt builders -> lessons_block()
  - app.py /lessons endpoints -> list_lessons() / delete_lesson()
"""
from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

_client = OpenAI(
    api_key=os.getenv("GEMINI_API_KEY"),
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
)
_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

_LESSONS_FILE = os.path.join(os.path.dirname(__file__), "learned_lessons.json")
_MAX_LESSONS_IN_PROMPT = 30
_lock = threading.Lock()


# =========================================================
# STORAGE
# =========================================================
def _load() -> list[dict]:
    try:
        if not os.path.exists(_LESSONS_FILE):
            return []
        with open(_LESSONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save(lessons: list[dict]) -> None:
    with open(_LESSONS_FILE, "w", encoding="utf-8") as f:
        json.dump(lessons, f, indent=2, ensure_ascii=False, default=str)


def list_lessons() -> list[dict]:
    with _lock:
        return _load()


def delete_lesson(lesson_id: str) -> bool:
    with _lock:
        lessons = _load()
        kept = [l for l in lessons if l.get("id") != lesson_id]
        if len(kept) == len(lessons):
            return False
        _save(kept)
        return True


def _words(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9_]+", text.lower()))


# A durable rule must stand on its own — anything referring to "the previous
# answer/data" is a conversation-specific dispute, not a lesson (one such junk
# rule, "The previous data provided was for IPG only.", got saved on 2026-07-03).
_CONTEXT_BOUND_PAT = re.compile(
    r"(?i)\b(?:the |your )?(?:previous|above|last|prior|earlier)\b.{0,30}"
    r"\b(?:answer|data|result|response|query|table|figures?)\b"
    r"|\bthat was\b|\byou (?:gave|showed|said|provided)\b")


def is_durable_rule(rule: str) -> bool:
    return bool(rule) and not _CONTEXT_BOUND_PAT.search(rule)


def save_lesson(rule: str, source_question: str = "") -> dict:
    """Save a rule; if a very similar rule already exists, replace it (the
    newest correction wins) instead of piling up near-duplicates."""
    rule = (rule or "").strip()
    if not rule or not is_durable_rule(rule):
        return {}
    with _lock:
        lessons = _load()
        new_words = _words(rule)
        kept = []
        for l in lessons:
            old_words = _words(l.get("rule", ""))
            overlap = len(new_words & old_words) / max(len(new_words | old_words), 1)
            if overlap < 0.6:  # keep only rules that are clearly different
                kept.append(l)
        entry = {
            "id": uuid.uuid4().hex[:12],
            "created_at": datetime.utcnow().isoformat(),
            "rule": rule,
            "source_question": source_question[:500],
        }
        kept.append(entry)
        _save(kept)
        return entry


def lessons_block() -> str:
    """Formatted block of learned rules for injection into LLM prompts.
    Returns "" when nothing has been learned yet."""
    lessons = list_lessons()
    if not lessons:
        return ""
    lines = [f"- {l['rule']}" for l in lessons[-_MAX_LESSONS_IN_PROMPT:] if l.get("rule")]
    if not lines:
        return ""
    return (
        "LEARNED RULES (the user explicitly taught these corrections — they OVERRIDE "
        "any conflicting default rule above; ALWAYS apply them):\n" + "\n".join(lines)
    )


# =========================================================
# CORRECTION DETECTION
# =========================================================
# Cheap keyword gate so the LLM classifier only runs on messages that could
# plausibly be teaching something (not on every question).
_CORRECTION_HINTS = (
    "should", "shud", "must", "always", "never", "wrong", "incorrect",
    "actually", "instead", "not like that", "remember", "from now on",
    "make sure", "don't", "dont", "do not", "the correct", "correct way",
    "learn", "note that", "keep in mind", "definition", "you have to",
    "use only", "not the", "that's not", "thats not",
)

_DETECT_PROMPT = """You watch messages sent to the WEBXPAY analytics chatbot and decide whether the
user is TEACHING the system a durable rule/correction (a definition, formula,
column/filter to use, business rule, or behavioral instruction that should be
remembered for ALL future questions).

Reply with EXACTLY one JSON object:
{"is_correction": true/false, "rule": "<the durable rule, rewritten as one clear standalone instruction>", "also_wants_data": true/false}

- is_correction=true ONLY when the message states how things should be defined,
  computed, or how the bot should behave going forward. A plain data question,
  a greeting, or a one-off request is NOT a correction.
- "rule" must be self-contained (mention the tables/columns/terms the user used;
  do not reference "the previous answer").
- also_wants_data=true when the message ALSO asks for data to be retrieved now
  (e.g. "active means is_active=1, now show me the list").

NOT corrections (is_correction=false):
- Plain data questions, greetings, one-off requests.
- DISPUTES about the previous answer that state a conversation-specific fact rather
  than a general rule: "no you are wrong that was only ipg", "that number is wrong",
  "the above is for june not july". These reference THE PREVIOUS ANSWER — they are
  conversation repairs, not durable knowledge. A durable rule must make sense to
  someone who never saw this conversation.

Examples:
"for active merchants you should check is_active = 1 and free_trail = 0"
-> {"is_correction": true, "rule": "An active merchant is defined as tbl_store.is_active = 1 AND tbl_store.free_trail = 0.", "also_wants_data": false}
"what was gmv last month" -> {"is_correction": false, "rule": "", "also_wants_data": false}
"no thats wrong, revenue must always exclude refunds. recalculate it"
-> {"is_correction": true, "rule": "Revenue must always exclude refunds.", "also_wants_data": true}
"no you are wrong that was only ipg"
-> {"is_correction": false, "rule": "", "also_wants_data": false}
"rm sales should always include both ipg and pos"
-> {"is_correction": true, "rule": "RM sales/performance rankings must combine approved IPG GMV and POS GMV (both channels) unless the user restricts to one channel.", "also_wants_data": false}"""


def detect_correction(question: str, history=None) -> dict:
    """Returns {"is_correction": bool, "rule": str, "also_wants_data": bool}.
    Cheap keyword gate first; LLM only runs when the message looks like teaching."""
    out = {"is_correction": False, "rule": "", "also_wants_data": False}
    ql = (question or "").lower()
    if not ql or not any(h in ql for h in _CORRECTION_HINTS):
        return out
    try:
        ctx = ""
        if isinstance(history, list) and history:
            last = history[-2:]
            ctx = "Recent conversation:\n" + "\n".join(
                f"{m.get('role', 'user')}: {str(m.get('content', ''))[:300]}" for m in last
            ) + "\n\n"
        resp = _client.chat.completions.create(
            model=_MODEL,
            temperature=0,
            max_tokens=300,
            reasoning_effort="none",  # disable Gemini thinking; else it eats the token budget
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _DETECT_PROMPT},
                {"role": "user", "content": ctx + f"Message: {question}"},
            ],
        )
        raw = (resp.choices[0].message.content or "").strip()
        obj = json.loads(raw)
        rule = (obj.get("rule") or "").strip() if isinstance(obj, dict) else ""
        if isinstance(obj, dict) and obj.get("is_correction") and is_durable_rule(rule):
            out["is_correction"] = True
            out["rule"] = rule
            out["also_wants_data"] = bool(obj.get("also_wants_data"))
    except Exception as e:
        print(f"[learning_store] detection failed: {e}")
    return out
