from openai import OpenAI
import os
import re
import json
import difflib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# =========================================================
# OPENAI CLIENT
# =========================================================
# NOTE: Do NOT hardcode API keys in code. Use an environment variable instead.
# export OPENAI_API_KEY="..."
api_key = os.getenv("OPENAI_API_KEY")

# =========================================================
# SCHEMA LOADER
# =========================================================
def load_schema() -> str:
    try:
        with open("schema.txt", "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        # Fallback minimal schema
        return "tbl_order, tbl_store, tbl_payment, tbl_exchange_rate, tbl_order_parent_gateway"


# =========================================================
# FEEDBACK CONTEXT LOADER
# =========================================================
_FEEDBACK_FILE = os.path.join(os.path.dirname(__file__), "feedback_log.json")

def is_known_wrong(question: str, threshold: float = 0.55) -> bool:
    """
    Returns True if this question closely matches a previously wrong-marked answer.
    Uses word-overlap ratio (Jaccard similarity) against the feedback log.
    """
    try:
        if not os.path.exists(_FEEDBACK_FILE):
            return False
        with open(_FEEDBACK_FILE, "r") as f:
            log = json.load(f)
    except Exception:
        return False

    ql_words = set(question.lower().split())
    for entry in log:
        if entry.get("correct") is not False:
            continue
        eq_words = set((entry.get("question") or "").lower().split())
        if not eq_words:
            continue
        intersection = len(ql_words & eq_words)
        union = len(ql_words | eq_words)
        if union > 0 and intersection / union >= threshold:
            return True
    return False


def load_wrong_feedback(question: str, max_entries: int = 5) -> str:
    """
    Returns a prompt snippet describing past wrong answers relevant to the
    current question, so the LLM avoids repeating the same mistakes.
    Only looks at entries marked correct=False.
    """
    try:
        if not os.path.exists(_FEEDBACK_FILE):
            return ""
        with open(_FEEDBACK_FILE, "r") as f:
            log = json.load(f)
    except Exception:
        return ""

    wrong = [e for e in log if e.get("correct") is False and e.get("question")]
    if not wrong:
        return ""

    # Simple relevance: score by word overlap with current question
    ql_words = set(question.lower().split())
    def relevance(entry):
        eq_words = set(entry["question"].lower().split())
        return len(ql_words & eq_words)

    ranked = sorted(wrong, key=relevance, reverse=True)[:max_entries]

    lines = [
        "⚠️  PAST MISTAKES — the user marked these answers as WRONG.",
        "You MUST generate a DIFFERENT SQL approach for similar questions.",
        "Do NOT reuse the same query structure that was marked wrong.",
    ]
    for e in ranked:
        sql_snippet = ""
        if e.get("sql"):
            sql_str = json.dumps(e["sql"]) if isinstance(e["sql"], dict) else str(e["sql"])
            sql_snippet = f"\n  Wrong SQL was: {sql_str[:400]}{'...' if len(sql_str) > 400 else ''}"
        lines.append(f'- Question: "{e["question"]}"{sql_snippet}')

    return "\n".join(lines)


def get_cached_sql(question: str, threshold: float = 0.72) -> str | None:
    """
    If the user previously marked a very similar question as CORRECT,
    reuse its SQL (substituting new dates if they differ).
    This skips the LLM SQL-generation step entirely — instant response.

    Returns the SQL string, or None if no good match found.
    """
    try:
        if not os.path.exists(_FEEDBACK_FILE):
            return None
        with open(_FEEDBACK_FILE, "r") as f:
            log = json.load(f)
    except Exception:
        return None

    correct_entries = [
        e for e in log
        if e.get("correct") is True
        and e.get("question")
        and e.get("sql")
        and isinstance(e.get("sql"), str)   # only plain SQL strings, not dicts
    ]
    if not correct_entries:
        return None

    ql_words = set(question.lower().split())

    best_score = 0.0
    best_entry = None
    for entry in correct_entries:
        eq_words = set(entry["question"].lower().split())
        if not eq_words:
            continue
        intersection = len(ql_words & eq_words)
        union = len(ql_words | eq_words)
        score = intersection / union if union > 0 else 0.0
        if score > best_score:
            best_score = score
            best_entry = entry

    if best_score < threshold or best_entry is None:
        return None

    cached_sql = best_entry["sql"]

    # ── Date substitution ──────────────────────────────────────────────────
    # If the new question has different dates, swap them into the cached SQL.
    try:
        new_ds, new_de = extract_date_range(question)
        old_ds, old_de = extract_date_range(best_entry["question"])
        if new_ds and new_de and old_ds and old_de and (new_ds != old_ds or new_de != old_de):
            cached_sql = cached_sql.replace(f"'{old_ds}'", f"'{new_ds}'")
            cached_sql = cached_sql.replace(f"'{old_de}'", f"'{new_de}'")
    except Exception:
        pass  # if date swap fails, still use the cached SQL as-is

    return cached_sql


# =========================================================
# MONTH + DATE UTILITIES
# =========================================================
MONTHS = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}


def month_range(year: int, month: int):
    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year+1}-01-01"
    else:
        end = f"{year}-{month+1:02d}-01"
    return start, end


def fuzzy_months(q: str):
    """Fuzzy detect all month words (handles typos like 'Novemebr')."""
    ql = q.lower()
    words = re.findall(r"[a-zA-Z]+", ql)
    months = []
    for w in words:
        match = difflib.get_close_matches(w, MONTHS.keys(), n=1, cutoff=0.7)
        if match:
            months.append(MONTHS[match[0]])
    return sorted(set(months))


def extract_date_range(q: str):
    """
    Universal date range extractor.
    Returns (start_date, end_date) strings (YYYY-MM-DD) or (None, None).
    """
    ql = q.lower()
    now = datetime.now()
    today = now.date()

    def fmt(d):
        return d.strftime("%Y-%m-%d")

    # ── Specific date literals ───────────────────────────────────────────────
    # M/D/YYYY or MM/DD/YYYY  e.g. "3/22/2026", "03/22/2026"
    m = re.search(r'\b(\d{1,2})/(\d{1,2})/(20\d{2})\b', q)
    if m:
        try:
            from datetime import date as _date
            d = _date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
            return fmt(d), fmt(d + timedelta(days=1))
        except ValueError:
            pass

    # YYYY-MM-DD  e.g. "2026-03-22"
    m = re.search(r'\b(20\d{2})-(\d{2})-(\d{2})\b', q)
    if m:
        try:
            from datetime import date as _date
            d = _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return fmt(d), fmt(d + timedelta(days=1))
        except ValueError:
            pass

    # DD/MM/YYYY  e.g. "22/03/2026"
    m = re.search(r'\b(\d{2})/(\d{2})/(20\d{2})\b', q)
    if m:
        try:
            from datetime import date as _date
            day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                d = _date(year, month, day)
                return fmt(d), fmt(d + timedelta(days=1))
        except ValueError:
            pass

    # "on March 22" / "on 22 March" / "March 22, 2026" etc.
    # Handled later via fuzzy month + day matching

    # ── Relative day phrases ─────────────────────────────────────────────────
    if "yesterday" in ql or "day before today" in ql:
        d = today - timedelta(days=1)
        return fmt(d), fmt(today)

    if "today" in ql or "right now" in ql:
        return fmt(today), fmt(today + timedelta(days=1))

    # ── Relative week phrases ────────────────────────────────────────────────
    if "last week" in ql or "previous week" in ql:
        # Monday→Sunday of last calendar week
        start_of_this_week = today - timedelta(days=today.weekday())
        start_of_last_week = start_of_this_week - timedelta(days=7)
        return fmt(start_of_last_week), fmt(start_of_this_week)

    if "this week" in ql or "current week" in ql:
        start_of_this_week = today - timedelta(days=today.weekday())
        return fmt(start_of_this_week), fmt(today + timedelta(days=1))

    # ── Relative quarter phrases ─────────────────────────────────────────────
    def quarter_range(year: int, q_num: int):
        start_month = (q_num - 1) * 3 + 1
        end_month   = start_month + 3
        s = f"{year}-{start_month:02d}-01"
        if end_month > 12:
            e = f"{year+1}-01-01"
        else:
            e = f"{year}-{end_month:02d}-01"
        return s, e

    current_q = (today.month - 1) // 3 + 1

    if "last quarter" in ql or "previous quarter" in ql:
        q_num = current_q - 1 if current_q > 1 else 4
        yr    = now.year    if current_q > 1 else now.year - 1
        return quarter_range(yr, q_num)

    if "this quarter" in ql or "current quarter" in ql:
        return quarter_range(now.year, current_q)

    # Named quarter + year: "Q1 2026", "q3 2025"
    m = re.search(r'\bq([1-4])\s*(20\d{2})\b', ql)
    if m:
        return quarter_range(int(m.group(2)), int(m.group(1)))

    # ── Standard relative phrases ────────────────────────────────────────────
    if "last year" in ql:
        y = now.year - 1
        return f"{y}-01-01", f"{y+1}-01-01"

    if "this year" in ql or "current year" in ql:
        return f"{now.year}-01-01", f"{now.year+1}-01-01"

    if "year to date" in ql or "ytd" in ql or "so far this year" in ql:
        return f"{now.year}-01-01", fmt(today)

    if "month to date" in ql or "mtd" in ql:
        return f"{now.year}-{now.month:02d}-01", fmt(today)

    if "last month" in ql or "previous month" in ql:
        year_val  = now.year  if now.month > 1 else now.year - 1
        month_val = now.month - 1 if now.month > 1 else 12
        return month_range(year_val, month_val)

    if "this month" in ql or "current month" in ql:
        return month_range(now.year, now.month)

    # last/past X days
    m = re.search(r"(?:last|past)\s+(\d+)\s+days?", ql)
    if m:
        days = int(m.group(1))
        start = today - timedelta(days=days)
        return fmt(start), fmt(today)

    # last X weeks
    m = re.search(r"last\s+(\d+)\s+weeks?", ql)
    if m:
        weeks = int(m.group(1))
        start = today - timedelta(weeks=weeks)
        return fmt(start), fmt(today)

    # last X months (approx)
    m = re.search(r"last\s+(\d+)\s+months?", ql)
    if m:
        months = int(m.group(1))
        start = today - timedelta(days=months * 30)
        return fmt(start), fmt(today)

    # ── Month + year patterns ─────────────────────────────────────────────────
    year_match  = re.search(r"(20\d{2})", ql)
    months_found = fuzzy_months(q)

    if year_match and len(months_found) >= 2:
        year    = int(year_match.group(1))
        m_start = months_found[0]
        m_end   = months_found[-1]
        start   = f"{year}-{m_start:02d}-01"
        end     = f"{year}-{m_end+1:02d}-01" if m_end < 12 else f"{year+1}-01-01"
        return start, end

    # Range "from X to Y 2025"
    mrange = re.search(
        r"(?:from|between)\s+([a-zA-Z]+)\s+(?:to|and|-)\s+([a-zA-Z]+)\s+(20\d{2})",
        ql,
    )
    if mrange:
        m1   = MONTHS.get(mrange.group(1).lower())
        m2   = MONTHS.get(mrange.group(2).lower())
        year = int(mrange.group(3))
        if m1 and m2:
            start_m = min(m1, m2)
            end_m   = max(m1, m2)
            start   = f"{year}-{start_m:02d}-01"
            end     = f"{year}-{end_m+1:02d}-01" if end_m < 12 else f"{year+1}-01-01"
            return start, end

    # ── Day-range within a month: "2nd to 5th January 2026" / "January 2nd to 5th 2026" ─────
    # Also handles typos via fuzzy month matching
    _ord_range_patterns = [
        # "Nth to Mth Month [Year]"  e.g. "2nd to 5th january 2026"
        re.search(
            r'(\d{1,2})(?:st|nd|rd|th)\s+(?:to|through|and|-)\s+(\d{1,2})(?:st|nd|rd|th)\s+([a-zA-Z]{3,})',
            q, re.IGNORECASE
        ),
        # "Month Nth to Mth [Year]"  e.g. "january 2nd to 5th 2026"
        re.search(
            r'([a-zA-Z]{3,})\s+(\d{1,2})(?:st|nd|rd|th)\s+(?:to|through|and|-)\s+(\d{1,2})(?:st|nd|rd|th)',
            q, re.IGNORECASE
        ),
    ]
    for _rp in _ord_range_patterns:
        if _rp:
            gs = _rp.groups()  # either (day1, day2, month_word) or (month_word, day1, day2)
            # determine which group is the month word
            _month_word = None
            _day1 = _day2 = None
            try:
                # pattern 1: (day1, day2, month_word)
                if gs[2].isalpha():
                    _day1, _day2, _month_word = int(gs[0]), int(gs[1]), gs[2]
                # pattern 2: (month_word, day1, day2)
                elif gs[0].isalpha():
                    _month_word, _day1, _day2 = gs[0], int(gs[1]), int(gs[2])
            except (ValueError, TypeError):
                pass
            if _month_word:
                # Try exact match first, then fuzzy
                _mn = MONTHS.get(_month_word.lower())
                if _mn is None:
                    _fm = difflib.get_close_matches(_month_word.lower(), MONTHS.keys(), n=1, cutoff=0.7)
                    _mn = MONTHS[_fm[0]] if _fm else None
                if _mn and _day1 and _day2:
                    try:
                        from datetime import date as _date
                        _yr = int(year_match.group(1)) if year_match else now.year
                        _d1 = _date(_yr, _mn, min(_day1, _day2))
                        _d2 = _date(_yr, _mn, max(_day1, _day2))
                        return fmt(_d1), fmt(_d2 + timedelta(days=1))
                    except (ValueError, AttributeError):
                        pass

    # Plain number range within month: "january 2 to 5 2026" / "2 to 5 january 2026"
    _plain_range_patterns = [
        re.search(r'(\d{1,2})\s+(?:to|through|-)\s+(\d{1,2})\s+([a-zA-Z]{3,})', q, re.IGNORECASE),
        re.search(r'([a-zA-Z]{3,})\s+(\d{1,2})\s+(?:to|through|-)\s+(\d{1,2})', q, re.IGNORECASE),
    ]
    for _pp in _plain_range_patterns:
        if _pp:
            gs = _pp.groups()
            _month_word = _day1 = _day2 = None
            try:
                if gs[2].isalpha():
                    _day1, _day2, _month_word = int(gs[0]), int(gs[1]), gs[2]
                elif gs[0].isalpha():
                    _month_word, _day1, _day2 = gs[0], int(gs[1]), int(gs[2])
            except (ValueError, TypeError):
                pass
            if _month_word:
                _mn = MONTHS.get(_month_word.lower())
                if _mn is None:
                    _fm = difflib.get_close_matches(_month_word.lower(), MONTHS.keys(), n=1, cutoff=0.7)
                    _mn = MONTHS[_fm[0]] if _fm else None
                # Sanity-check: values must look like days (1-31), not years or big numbers
                if _mn and _day1 and _day2 and 1 <= _day1 <= 31 and 1 <= _day2 <= 31:
                    try:
                        from datetime import date as _date
                        _yr = int(year_match.group(1)) if year_match else now.year
                        _d1 = _date(_yr, _mn, min(_day1, _day2))
                        _d2 = _date(_yr, _mn, max(_day1, _day2))
                        return fmt(_d1), fmt(_d2 + timedelta(days=1))
                    except (ValueError, AttributeError):
                        pass

    # "DDth Month YYYY" / "Month DDth YYYY" — ordinal day (1st, 2nd, 3rd, 5th, 22nd …)
    ord_day_month = re.search(r'\b(\d{1,2})(?:st|nd|rd|th)\s+([a-zA-Z]{3,})\b', q, re.IGNORECASE)
    ord_month_day = re.search(r'\b([a-zA-Z]{3,})\s+(\d{1,2})(?:st|nd|rd|th)\b', q, re.IGNORECASE)
    for pattern, day_g, month_g in [(ord_day_month, 1, 2), (ord_month_day, 2, 1)]:
        if pattern:
            mn = MONTHS.get(pattern.group(month_g).lower())
            if mn is None:
                _fm = difflib.get_close_matches(pattern.group(month_g).lower(), MONTHS.keys(), n=1, cutoff=0.7)
                mn = MONTHS[_fm[0]] if _fm else None
            if mn:
                try:
                    day_num = int(pattern.group(day_g))
                    yr = int(year_match.group(1)) if year_match else now.year
                    from datetime import date as _date
                    d = _date(yr, mn, day_num)
                    return fmt(d), fmt(d + timedelta(days=1))
                except (ValueError, AttributeError):
                    pass

    # "Month DD" or "DD Month" with optional year: "March 22", "22 March 2026"
    day_month = re.search(r'\b(\d{1,2})\s+([a-zA-Z]{3,})\b', q)
    month_day = re.search(r'\b([a-zA-Z]{3,})\s+(\d{1,2})\b', q)
    for pattern, day_g, month_g in [(day_month, 1, 2), (month_day, 2, 1)]:
        if pattern:
            mn = MONTHS.get(pattern.group(month_g).lower())
            if mn:
                try:
                    day_num = int(pattern.group(day_g))
                    yr = int(year_match.group(1)) if year_match else now.year
                    from datetime import date as _date
                    d = _date(yr, mn, day_num)
                    return fmt(d), fmt(d + timedelta(days=1))
                except (ValueError, AttributeError):
                    pass

    # Single month + year
    if year_match and len(months_found) == 1:
        year = int(year_match.group(1))
        return month_range(year, months_found[0])

    # Year only
    if year_match:
        year = int(year_match.group(1))
        return f"{year}-01-01", f"{year+1}-01-01"

    # Month only (no year) — assume most recent occurrence of that month
    if len(months_found) == 1:
        mn = months_found[0]
        # If the month is in the future this year, use last year; otherwise use this year
        if mn > today.month:
            year = now.year - 1
        else:
            year = now.year
        return month_range(year, mn)

    return None, None


# =========================================================
# CURRENCY HELPERS
# =========================================================
CURRENCY_MAP = {
    "usd": 2,
    "dollar": 2,
    "dollars": 2,
    "lkr": 5,
    "rs": 5,
    "rupee": 5,
    "rupees": 5,
    "gbp": 1,
    "pound": 1,
    "pounds": 1,
    "eur": 3,
    "euro": 3,
    "euros": 3,
    "aud": 6,
    "australian": 6,
}

# Optional: nice labels per currency_id for column aliases
CURRENCY_LABEL = {
    1: "gbp",
    2: "usd",
    3: "eur",
    5: "lkr",
    6: "aud",
}


def detect_currency_ids(question: str):
    """Return list of *all* currency_ids mentioned in the question."""
    ql = question.lower()
    ids = []
    for token, cid in CURRENCY_MAP.items():
        if token in ql and cid not in ids:
            ids.append(cid)
    return ids


def primary_currency_from_question(question: str):
    """Return the first detected currency_id + name (for backwards compatibility)."""
    ql = question.lower()
    for token, cid in CURRENCY_MAP.items():
        if token in ql:
            return cid, token
    return None, None


# =========================================================
# INTENT ANALYSIS
# =========================================================
def analyze_intent(question: str):
    ql = question.lower()
    intent = {
        "type": "generic",  # 'revenue', 'gmv', 'mdr', 'volume', 'count', 'schema', 'generic'
        "metric": None,
        "date_start": None,
        "date_end": None,
        "currency_id": None,
        "currency_name": None,

        # ✅ ADD: time granularity (applies to BOTH IPG + POS)
        "time_grain": None,   # "day" | "week" | "month"

        # ✅ ADD: POS routing fields
        "channel": None,     # e.g. "pos" , "dfcc" , "amex"
        "pos_metric": None,  # "all" | "gmv" | "count" | "dfcc_rev" | "amex_rev" | "total_rev"
    }

    # ✅ Detect requested breakdown granularity (daily/weekly/monthly)
    if any(k in ql for k in ["daily", "day wise", "day-wise", "per day", "by day", "each day"]):
        intent["time_grain"] = "day"
    elif any(k in ql for k in ["weekly", "week wise", "week-wise", "per week", "by week", "each week"]):
        intent["time_grain"] = "week"
    elif any(k in ql for k in ["monthly", "month wise", "month-wise", "per month", "by month", "each month"]):
        intent["time_grain"] = "month"

    # ✅ CHANGE: POS implied detection so "dfcc rev ..." or "hnb rev ..." routes to POS even without word "pos"
    pos_implied = False
    if ("dfcc" in ql or "hnb" in ql) and ("rev" in ql or "revenue" in ql):
        pos_implied = True

    # ✅ POS detection (separate from online gateway logic)
    if "pos" in ql or "tbl_pos_transactions" in ql or pos_implied:

        intent["channel"] = "pos"

        # ✅ CHANGE: dfcc + hnb (not amex)
        if "dfcc" in ql and ("revenue" in ql or "rev" in ql):
            intent["pos_metric"] = "dfcc_rev"
        elif "hnb" in ql and ("revenue" in ql or "rev" in ql):
            intent["pos_metric"] = "hnb_rev"
        elif ("total" in ql or "overall" in ql) and ("revenue" in ql or "rev" in ql):
            intent["pos_metric"] = "total_rev"
        elif (
            ("transaction" in ql and ("count" in ql or "how many" in ql or "number of" in ql))
            or ("txn" in ql and "count" in ql)
        ):
            intent["pos_metric"] = "count"
        elif "gmv" in ql or "sales" in ql or "volume" in ql or "turnover" in ql:
            intent["pos_metric"] = "gmv"
        else:
            intent["pos_metric"] = "all"

        intent["type"] = "pos"

    # detect any currency word
    has_currency_word = any(token in ql for token in CURRENCY_MAP.keys())

    # Volume questions: treat "volume" and "total transactions" the same (approved count)
    if (
        "volume of transaction" in ql
        or "volume of transactions" in ql
        or "total volume of transaction" in ql
        or "transaction volume" in ql
        or ("volume" in ql and "transaction" in ql)
        or "total transactions" in ql
        # also "usd volume", "lkr volume" etc. (but not "sales volume")
        or ("volume" in ql and has_currency_word and "sales volume" not in ql)
    ):
        intent["type"] = "volume"
        intent["metric"] = "count"

    # Metric / type (only if not already classified as volume)
    if intent["type"] == "generic":
        # Explicit MDR (overall MDR, not "bank mdr")
        if "mdr" in ql and "bank" not in ql:
            intent["type"] = "mdr"
            intent["metric"] = "mdr"
        elif "revenue" in ql or "profit" in ql or "margin" in ql:
            intent["type"] = "revenue"
            intent["metric"] = "revenue"
        elif (
            "gmv"
            in ql
            or "sales volume" in ql
            or "turnover" in ql
            or "total amount" in ql
            or "amount" in ql
            or "gross amount" in ql
            or "amount collected" in ql
            or "value" in ql  # e.g. "total lkr value"
        ):
            intent["type"] = "gmv"
            intent["metric"] = "gmv"
        elif (
            "how many" in ql
            or "count" in ql
            or "number of" in ql
            or "transaction count" in ql
            or "total transaction" in ql  # e.g. "total transaction for october 2025"
        ):
            intent["type"] = "count"
            intent["metric"] = "count"

    if (
        "show tables" in ql
        or "list tables" in ql
        or "columns in" in ql
        or "schema" in ql
    ):
        intent["type"] = "schema"

    # Date range
    ds, de = extract_date_range(question)
    intent["date_start"], intent["date_end"] = ds, de

    # Primary currency (for revenue / legacy)
    cid, cname = primary_currency_from_question(question)
    intent["currency_id"] = cid
    intent["currency_name"] = cname

    return intent



# =========================================================
# ✅ POS TIME SERIES SQL (PBI-MATCH, NO extra columns)
# =========================================================
def build_pos_sql(intent: dict) -> str:
    ds = intent.get("date_start")
    de = intent.get("date_end")
    metric = intent.get("pos_metric") or "all"

    date_filter = ""
    date_cond_as = ""
    if ds and de:
        date_filter = (
            f"WHERE t.transaction_date >= '{ds}' "
            f"  AND t.transaction_date <  '{de}'"
        )
        date_cond_as = f"AND transaction_date >= '{ds}' AND transaction_date < '{de}'"

    # ─────────────────────────────────────────────────────────────────────────
    # Single date-filtered pair subquery (pk_as) for GMV and count.
    # Revenue uses no pair key — simple adjusted_amount via tbl_pos_store_bank_mid.
    # ─────────────────────────────────────────────────────────────────────────
    pk_inner = """CONCAT(
            COALESCE(TRIM(CAST(invoice_no  AS CHAR)),''),'|',
            COALESCE(TRIM(CAST(auth_code   AS CHAR)),''),'|',
            COALESCE(TRIM(rrn),''),'|',
            COALESCE(TRIM(CAST(terminal_id AS CHAR)),''),'|',
            COALESCE(TRIM(CAST(terminal_sn AS CHAR)),'')
        )"""

    pair_key_t = """CONCAT(
        COALESCE(TRIM(CAST(t.invoice_no  AS CHAR)),''),'|',
        COALESCE(TRIM(CAST(t.auth_code   AS CHAR)),''),'|',
        COALESCE(TRIM(t.rrn),''),'|',
        COALESCE(TRIM(CAST(t.terminal_id AS CHAR)),''),'|',
        COALESCE(TRIM(CAST(t.terminal_sn AS CHAR)),'')
    )"""

    pk_as_join = f"""LEFT JOIN (
    SELECT ipg_provider_id, pair_key
    FROM (
        SELECT ipg_provider_id,
               {pk_inner} AS pair_key,
               LOWER(TRIM(COALESCE(txn_type,''))) AS txn_norm
        FROM webxpay_master.tbl_pos_transactions
        WHERE ipg_provider_id IN (5, 6) {date_cond_as}
    ) _pas
    GROUP BY ipg_provider_id, pair_key
    HAVING SUM(CASE WHEN txn_norm IN ('sale','amex')             THEN 1 ELSE 0 END) > 0
       AND SUM(CASE WHEN txn_norm IN ('','void_sale','void_amex','void-sale','void-amex') THEN 1 ELSE 0 END) > 0
) pk_as
    ON  t.ipg_provider_id = pk_as.ipg_provider_id
    AND {pair_key_t} = pk_as.pair_key"""

    txn_norm = "LOWER(TRIM(COALESCE(t.txn_type,'')))"

    # ── Total POS GMV (PVI 5+6) — "Total Amount" DAX (ALLSELECTED pair via pk_as) ──
    gmv_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','','void_sale','void_amex','void-sale','void-amex') THEN NULL
        WHEN pk_as.pair_key IS NOT NULL THEN NULL           -- paired → eliminate
        WHEN {txn_norm} IN ('sale','amex') THEN t.amount   -- unpaired sale
        ELSE NULL                                            -- voids: HasKey always TRUE → blank
    END
) AS pos_gmv_lkr"""

    # ── HNB GMV (PVI 5) — simple sum of LKR sales (DAX: Total Amount PVI 5) ──────
    hnb_gmv_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id = 5
         AND t.currency = 'LKR'
         AND t.amount IS NOT NULL
         AND {txn_norm} IN ('sale','amex')
        THEN t.amount ELSE NULL
    END
) AS hnb_gmv_lkr"""

    # ── DFCC GMV (PVI 6) — "Total Amount (PVI 6 Only)" DAX (date-filtered pair via pk_as) ──
    dfcc_gmv_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id <> 6 THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','','void_sale','void_amex','void-sale','void-amex') THEN NULL
        WHEN pk_as.pair_key IS NOT NULL THEN NULL           -- paired → eliminate
        WHEN {txn_norm} IN ('sale','amex') THEN t.amount   -- unpaired sale
        ELSE NULL                                            -- voids: NULL
    END
) AS dfcc_gmv_lkr"""

    # ── HNB Volume (PVI 5) — unpaired sales only (date-filtered pk_as)
    hnb_count_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id <> 5 THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} IN ('sale','amex') THEN
            CASE WHEN pk_as.pair_key IS NOT NULL THEN NULL ELSE 1 END
        ELSE NULL
    END
) AS hnb_volume"""

    # ── DFCC Volume (PVI 6) — unpaired sales only (date-filtered pk_as)
    dfcc_count_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id <> 6 THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} IN ('sale','amex') THEN
            CASE WHEN pk_as.pair_key IS NOT NULL THEN NULL ELSE 1 END
        ELSE NULL
    END
) AS dfcc_volume"""

    # ── Total Volume (PVI 5+6) — unpaired sales only (date-filtered pk_as)
    count_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} IN ('sale','amex') THEN
            CASE WHEN pk_as.pair_key IS NOT NULL THEN NULL ELSE 1 END
        ELSE NULL
    END
) AS Transaction_Count"""

    # ── Revenue: adjusted_amount × (m.mdr_rate − m.cost_rate) / 100 ─────────────────
    # sale/amex → +amount, void_sale/void_amex → −amount (direct net, no pair key needed)
    # Joined via tbl_pos_store_bank_mid on store_id + bank_merchant_mid.
    # Transactions with no matching store_mid entry → m.mdr_rate = 0 → revenue = 0.
    adj_amt = f"""CASE
        WHEN {txn_norm} IN ('sale','amex')                               THEN  t.amount
        WHEN {txn_norm} IN ('void_sale','void_amex','void-sale','void-amex') THEN -t.amount
        ELSE 0
    END"""

    row_rev = f"""ROUND(
        ({adj_amt}) * (COALESCE(m.mdr_rate, 0) - COALESCE(m.cost_rate, 0)) / 100.0
    , 2)"""

    # ── DFCC Revenue (PVI 6) ────────────────────────────────────────────────────────
    dfcc_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id <> 6 THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','void_sale','void_amex','void-sale','void-amex') THEN NULL
        ELSE {row_rev}
    END
) AS DFCC_POS_revenue"""

    # ── HNB Revenue (PVI 5) ─────────────────────────────────────────────────────────
    hnb_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id <> 5 THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','void_sale','void_amex','void-sale','void-amex') THEN NULL
        ELSE {row_rev}
    END
) AS HNB_POS_revenue"""

    # ── Total Revenue (PVI 5+6) ─────────────────────────────────────────────────────
    total_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','void_sale','void_amex','void-sale','void-amex') THEN NULL
        ELSE {row_rev}
    END
) AS pos_total_revenue_lkr"""

    if metric == "gmv":
        select_clause = ",\n\n    ".join([gmv_expr, hnb_gmv_expr, dfcc_gmv_expr])
    elif metric == "count":
        select_clause = ",\n\n    ".join([count_expr, hnb_count_expr, dfcc_count_expr])
    elif metric == "dfcc_rev":
        select_clause = dfcc_expr
    elif metric == "hnb_rev":
        select_clause = hnb_expr
    elif metric == "total_rev":
        select_clause = total_expr
    elif metric == "dfcc_gmv":
        select_clause = dfcc_gmv_expr
    elif metric == "hnb_gmv":
        select_clause = hnb_gmv_expr
    elif metric == "dfcc_count":
        select_clause = dfcc_count_expr
    elif metric == "hnb_count":
        select_clause = hnb_count_expr
    else:  # "all"
        select_clause = ",\n\n    ".join([
            gmv_expr, hnb_gmv_expr, dfcc_gmv_expr,
            count_expr, hnb_count_expr, dfcc_count_expr,
            dfcc_expr, hnb_expr, total_expr,
        ])

    # Join tbl_pos_store_bank_mid for mdr_rate and cost_rate used in revenue calculation.
    store_mid_join = """LEFT JOIN webxpay_master.tbl_pos_store_bank_mid m
    ON  m.store_id          = t.store_id
    AND m.bank_merchant_mid = t.bank_merchant_mid
    AND m.is_active         = 1"""

    # Merchant grouping: any question that mentions "merchant" or asks for top/best/ranking,
    # or uses a threshold like "below 350k", "above 1m", "less than 500000"
    _threshold_pattern = re.compile(
        r'(below|under|less than|above|over|more than|greater than)\s+'
        r'([\d,]+(?:\.\d+)?)\s*([km]?)',
        re.IGNORECASE
    )

    def _parse_threshold(q: str):
        """Returns (operator, value) or (None, None)."""
        m = _threshold_pattern.search(q)
        if not m:
            return None, None
        direction = m.group(1).lower()
        raw = m.group(2).replace(',', '')
        suffix = m.group(3).lower()
        value = float(raw)
        if suffix == 'k':
            value *= 1_000
        elif suffix == 'm':
            value *= 1_000_000
        op = '<' if direction in ('below', 'under', 'less than') else '>'
        return op, int(value)

    def _is_pos_merchant_query(q: str) -> bool:
        ql = q.lower()
        if not any(w in ql for w in ["merchant", "store", "shop"]):
            return False
        if _threshold_pattern.search(ql):
            return True
        return any(w in ql for w in [
            "which", "who", "top", "best", "worst", "rank", "highest", "lowest",
            "each", "per", "by", "all", "list", "breakdown", "wise", "compare",
        ])

    _question_raw = intent.get("question", "")
    merchant_grouping = _is_pos_merchant_query(_question_raw)
    txn_threshold_op, txn_threshold_val = _parse_threshold(_question_raw.lower())
    having_clause = ""
    if txn_threshold_op and txn_threshold_val:
        having_clause = f"HAVING Transaction_Count {txn_threshold_op} {txn_threshold_val}"

    if merchant_grouping:
        store_join = "LEFT JOIN webxpay_master.tbl_store s ON s.store_id = t.store_id"
        # Pick ORDER BY column that actually exists in the select clause
        metric_order_map = {
            "hnb_rev":   "HNB_POS_revenue",
            "dfcc_rev":  "DFCC_POS_revenue",
            "total_rev": "pos_total_revenue_lkr",
            "gmv":       "pos_gmv_lkr",
            "hnb_gmv":   "hnb_gmv_lkr",
            "dfcc_gmv":  "dfcc_gmv_lkr",
            "count":     "Transaction_Count",
            "hnb_count": "hnb_volume",
            "dfcc_count":"dfcc_volume",
            "all":       "pos_total_revenue_lkr",
        }
        order_col = metric_order_map.get(metric, "pos_total_revenue_lkr")
        sql = f"""
SELECT
    t.store_id,
    s.doing_business_name,
    {select_clause}
FROM webxpay_master.tbl_pos_transactions t
{pk_as_join}
{store_mid_join}
{store_join}
{date_filter}
GROUP BY t.store_id, s.doing_business_name
{having_clause}
ORDER BY {order_col} DESC;
""".strip()
    else:
        sql = f"""
SELECT
    {select_clause}
FROM webxpay_master.tbl_pos_transactions t
{pk_as_join}
{store_mid_join}
{date_filter};
""".strip()

    return sql

# =========================================================
# REVENUE SQL
# =========================================================
def build_revenue_sql(intent: dict) -> str:
    """
    Revenue queries.

    IMPORTANT:
    - GMV in this function is always expressed in LKR:
        * LKR GMV + USD GMV converted to LKR + other currencies converted to LKR
      using the same base_lkr logic used everywhere else.
    """
    ds = intent["date_start"]
    de = intent["date_end"]
    cid = intent["currency_id"]

    date_filter = ""
    if ds and de:
        date_filter = (
            f"AND p.date_time_transaction >= '{ds}' "
            f"AND p.date_time_transaction < '{de}'"
        )

    currency_filter = ""
    if cid:
        currency_filter = f"AND o.processing_currency_id = '{cid}'"

    # Special USD path (explicit USD revenue)
    if cid == 2:
        sql = f"""
SELECT 
    COUNT(*) AS transaction_count,

    SUM(o.total_amount) AS total_gmv_usd,

    SUM(
        ROUND(
            o.total_amount * (
                CAST(o.payment_gateway_rate AS DECIMAL(10,4)) -
                CASE
                    WHEN o.order_type_id = 3 
                        THEN (CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4)) + COALESCE(opg.parent_gateway_rate, 0))
                    ELSE CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4))
                END
            ) / 100.0
        , 6)
    ) AS total_revenue_usd,

    SUM(
        (
            CASE
                WHEN o.processing_currency_id = '5' THEN o.total_amount

                WHEN o.exchange_rate IS NOT NULL AND o.exchange_rate <> '' 
                     AND o.exchange_rate REGEXP '^[0-9.]+$'
                THEN o.total_amount * CAST(o.exchange_rate AS DECIMAL(18,6))

                ELSE o.total_amount * (
                    SELECT er.buying_rate 
                    FROM webxpay_master.tbl_exchange_rate er
                    WHERE er.currency_id = o.processing_currency_id
                      AND er.date <= DATE(p.date_time_transaction)
                    ORDER BY er.date DESC
                    LIMIT 1
                )
            END
        ) * (
            (
                CAST(o.payment_gateway_rate AS DECIMAL(10,4)) -
                CASE 
                    WHEN o.order_type_id = 3 
                        THEN (CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4)) + COALESCE(opg.parent_gateway_rate,0))
                    ELSE CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4))
                END
            ) / 100.0
        )
    ) AS total_revenue_lkr

FROM webxpay_master.tbl_order o
JOIN webxpay_master.tbl_payment p 
      ON p.payment_id = o.payment_id
LEFT JOIN webxpay_master.tbl_order_parent_gateway opg 
      ON opg.order_id = o.order_id
WHERE 
    o.payment_status_id = 2
    AND o.processing_currency_id = '2'
    {date_filter};
"""
        return sql.strip()

    # LKR / other currencies path (overall revenue; GMV must be in LKR)
    fx = """
        SELECT er.buying_rate FROM tbl_exchange_rate er
        WHERE er.currency_id = o.processing_currency_id
          AND er.date <= DATE(p.date_time_transaction)
        ORDER BY er.date DESC LIMIT 1
    """

    base_lkr = f"""
        CASE
            WHEN o.processing_currency_id = '5' THEN o.total_amount
            ELSE
                CASE 
                    WHEN o.exchange_rate IS NOT NULL 
                         AND o.exchange_rate NOT LIKE '' 
                         AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
                    THEN o.total_amount * o.exchange_rate
                    ELSE o.total_amount * ({fx})
                END
        END
    """

    fee = """
        (
            o.payment_gateway_rate -
            CASE 
                WHEN o.order_type_id = 3 
                    THEN (CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4)) + COALESCE(opg.parent_gateway_rate,0))
                ELSE CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4))
            END
        ) / 100.0
    """

    # IMPORTANT: Revenue is base_lkr * fee, summed, rounded at the end
    rev_lkr_raw = f"(({base_lkr}) * ({fee}))"

    sql = f"""
SELECT
    -- GMV in LKR (LKR + USD→LKR + other FX→LKR)
    IFNULL(SUM(ROUND({base_lkr}, 2)), 0) AS total_gmv_lkr,
    -- Revenue in LKR based on the same LKR GMV base
    IFNULL(ROUND(SUM({rev_lkr_raw})), 0) AS total_revenue_lkr
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
LEFT JOIN tbl_order_parent_gateway opg ON opg.order_id = o.order_id
WHERE o.payment_status_id = 2
    {currency_filter}
    {date_filter};
"""
    return sql.strip()


# =========================================================
# MDR SQL (TOTAL MDR = BASE_LKR * PG_RATE, ROUNDED PER TXN)
# =========================================================
def build_mdr_sql(intent: dict) -> str:
    """
    Handles "total mdr ..." style questions (overall MDR, not bank-only).
    MDR in LKR = ROUND(base amount in LKR * (payment_gateway_rate / 100), 2) per transaction, then summed.
    """
    ds = intent["date_start"]
    de = intent["date_end"]
    cid = intent["currency_id"]

    date_filter = ""
    if ds and de:
        date_filter = (
            f"AND p.date_time_transaction >= '{ds}' "
            f"AND p.date_time_transaction < '{de}'"
        )

    currency_filter = ""
    if cid:
        currency_filter = f"AND o.processing_currency_id = '{cid}'"

    fx = """
        SELECT er.buying_rate FROM tbl_exchange_rate er
        WHERE er.currency_id = o.processing_currency_id
          AND er.date <= DATE(p.date_time_transaction)
        ORDER BY er.date DESC LIMIT 1
    """

    base_lkr = f"""
        CASE
            WHEN o.processing_currency_id = '5' THEN o.total_amount
            ELSE
                CASE 
                    WHEN o.exchange_rate IS NOT NULL 
                         AND o.exchange_rate NOT LIKE '' 
                         AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
                    THEN o.total_amount * o.exchange_rate
                    ELSE o.total_amount * ({fx})
                END
        END
    """

    # round MDR per transaction to 2 decimals, then sum
    mdr_expr = f"ROUND(({base_lkr}) * (CAST(o.payment_gateway_rate AS DECIMAL(10,4)) / 100.0), 2)"

    sql = f"""
SELECT
    SUM({mdr_expr}) AS total_mdr_lkr
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
LEFT JOIN tbl_order_parent_gateway opg ON opg.order_id = o.order_id
WHERE o.payment_status_id = 2
  {currency_filter}
  {date_filter};
"""
    return sql.strip()


# =========================================================
# GMV / VALUE SQL (AMOUNT)
# =========================================================
def _has_merchant_grouping(q: str) -> bool:
    ql = q.lower()
    return any(
        phrase in ql
        for phrase in [
            "each merchant",
            "per merchant",
            "by merchant",
            "merchant wise",
            "merchant-wise",
            "merchantwise",
        ]
    )


def build_gmv_sql(question: str, intent: dict) -> str:
    """
    Handles GMV / value style questions:
      - total lkr value october 2025
      - total usd value october 2025
      - total usd and lkr value october 2025
      - total amount from january to october 2025

    IMPORTANT BUSINESS RULE:
    - When combining currencies into a single "total amount" / "total GMV",
      we MUST convert everything to LKR and sum:
        total_lkr_gmv + (usd_gmv converted to LKR) [+ other FX]
      i.e. use base_lkr, NEVER raw SUM(o.total_amount) across currencies.

    - When the user EXPLICITLY asks for "LKR GMV only" or "USD GMV only",
      we can sum o.total_amount for that processing_currency_id.
    """
    ds = intent["date_start"]
    de = intent["date_end"]

    date_filter = ""
    if ds and de:
        date_filter = (
            f"AND p.date_time_transaction >= '{ds}' "
            f"AND p.date_time_transaction < '{de}'"
        )

    currency_ids = detect_currency_ids(question)

    # FX + base_lkr expression (same logic as revenue/MDR)
    fx = """
        SELECT er.buying_rate FROM tbl_exchange_rate er
        WHERE er.currency_id = o.processing_currency_id
          AND er.date <= DATE(p.date_time_transaction)
        ORDER BY er.date DESC LIMIT 1
    """

    base_lkr = f"""
        CASE
            WHEN o.processing_currency_id = '5' THEN o.total_amount
            ELSE
                CASE 
                    WHEN o.exchange_rate IS NOT NULL 
                         AND o.exchange_rate NOT LIKE '' 
                         AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
                    THEN o.total_amount * o.exchange_rate
                    ELSE o.total_amount * ({fx})
                END
        END
    """

    # Merchant-wise GMV: "for each merchant", "by merchant", etc.
    if _has_merchant_grouping(question):
        currency_filter = ""
        if currency_ids:
            ids_str = ",".join(f"'{c}'" for c in currency_ids)
            currency_filter = f"AND o.processing_currency_id IN ({ids_str})"

        # Merchant GMV is always in LKR-equivalent
        return f"""
SELECT 
    s.store_id,
    s.doing_business_name,
    SUM(ROUND({base_lkr}, 2)) AS total_gmv_lkr
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
JOIN tbl_store s ON s.store_id = o.store_id
WHERE o.payment_status_id = 2
  {currency_filter}
  {date_filter}
GROUP BY s.store_id, s.doing_business_name
ORDER BY total_gmv_lkr DESC;
""".strip()

    # No currency mentioned → total GMV in LKR (LKR + USD converted to LKR) — matches DAX All_Value
    if not currency_ids:
        return f"""
SELECT
    SUM(ROUND({base_lkr}, 2)) AS total_gmv_lkr
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND o.processing_currency_id IN ('5','2')
  {date_filter};
""".strip()

    # One currency → simple sum in that currency (explicit request: LKR-only / USD-only GMV)
    if len(currency_ids) == 1:
        cid = currency_ids[0]
        code = CURRENCY_LABEL.get(cid, f"cur_{cid}")
        label = f"total_{code}_value"

        return f"""
SELECT
    SUM(o.total_amount) AS {label}
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND o.processing_currency_id = '{cid}'
  {date_filter};
""".strip()

    # Multiple currencies (e.g. usd and lkr) → per-currency native + combined LKR
    select_parts = []
    filter_ids = ",".join(f"'{c}'" for c in currency_ids)

    for cid in currency_ids:
        code = CURRENCY_LABEL.get(cid, f"cur_{cid}")
        col_name = f"total_{code}_value"
        select_parts.append(
            f"SUM(CASE WHEN o.processing_currency_id = '{cid}' THEN o.total_amount ELSE 0 END) AS {col_name}"
        )

    # Combined LKR GMV across all mentioned currencies (convert then sum)
    select_parts.append(f"SUM(ROUND({base_lkr}, 2)) AS total_value_all_currencies_lkr")

    select_clause = ",\n    ".join(select_parts)

    return f"""
SELECT
    {select_clause}
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND o.processing_currency_id IN ({filter_ids})
  {date_filter};
""".strip()


# =========================================================
# VOLUME SQL (TRANSACTION COUNT)
# =========================================================
def build_volume_sql(question: str, intent: dict) -> str:
    """
    Handles volume (transaction count) style questions:
      - total usd volume october 2025
      - total lkr volume october 2025
      - total usd and lkr volume october 2025
      - total transaction count october 2025
      - total volume of transaction for october 2025
      - usd volume september 2025
    Uses processing_currency_id and p.date_time_transaction.
    Always filters approved (payment_status_id = 2).
    """
    ds = intent["date_start"]
    de = intent["date_end"]

    date_filter = ""
    if ds and de:
        date_filter = (
            f"AND p.date_time_transaction >= '{ds}' "
            f"AND p.date_time_transaction < '{de}'"
        )

    currency_ids = detect_currency_ids(question)

    # Merchant-wise volume
    if _has_merchant_grouping(question):
        currency_filter = ""
        if currency_ids:
            ids_str = ",".join(str(c) for c in currency_ids)
            currency_filter = f"AND o.processing_currency_id IN ({ids_str})"

        return f"""
SELECT
    s.store_id,
    s.doing_business_name,
    COUNT(*) AS txn_volume
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
JOIN tbl_store s ON s.store_id = o.store_id
WHERE o.payment_status_id = 2
  {currency_filter}
  {date_filter}
GROUP BY s.store_id, s.doing_business_name
ORDER BY txn_volume DESC;
""".strip()

    # No currency mentioned → LKR + USD volume — matches DAX Transaction_Count_All
    if not currency_ids:
        return f"""
SELECT
    COUNT(*) AS total_volume
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND o.processing_currency_id IN ('5','2')
  {date_filter};
""".strip()

    # One currency → simple volume
    if len(currency_ids) == 1:
        cid = currency_ids[0]
        code = CURRENCY_LABEL.get(cid, f"cur_{cid}")
        label = f"total_{code}_volume"

        return f"""
SELECT
    COUNT(*) AS {label}
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND o.processing_currency_id = '{cid}'
  {date_filter};
""".strip()

    # Multiple currencies (e.g. "usd and lkr volume") → per-currency and combined
    select_parts = []
    for cid in currency_ids:
        code = CURRENCY_LABEL.get(cid, f"cur_{cid}")
        col_name = f"total_{code}_volume"
        select_parts.append(
            f"SUM(CASE WHEN o.processing_currency_id = '{cid}' THEN 1 ELSE 0 END) AS {col_name}"
        )

    # Combined volume across all mentioned currencies
    select_parts.append("COUNT(*) AS total_volume")

    select_clause = ",\n    ".join(select_parts)
    ids_str = ",".join(f"'{c}'" for c in currency_ids)

    return f"""
SELECT
    {select_clause}
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND o.processing_currency_id IN ({ids_str})
  {date_filter};
""".strip()


# =========================================================
# BUSINESS LOGIC LAYER (NON-REVENUE SHORTCUTS)
# =========================================================
def build_txn_status_sql(question: str, ds: str, de: str) -> str:
    """
    IPG transaction status breakdown:
      payment_status_id: 1=Abandoned, 2=Approved, 3=Declined, 4=Cancelled

    Matches PowerBI DAX:
      ApprovedOrderCount  = COUNT(order_id) WHERE payment_status_id = 2
      DeclinedOrderCount  = COUNT(order_id) WHERE payment_status_id = 3
      App_Dec_Total       = COUNT(order_refference_number) WHERE payment_status_id <> 1
        (excludes abandoned from the denominator for approval/decline rate)

    Single-status, no percentage → simple COUNT(o.order_id) query.
    Multi-status or percentage → multi-column CASE WHEN breakdown.
    """
    ql = question.lower()

    date_filter = ""
    if ds and de:
        date_filter = (
            f"AND p.date_time_transaction >= '{ds}' "
            f"AND p.date_time_transaction < '{de}'"
        )

    # Detect which statuses the user cares about
    want_approved  = any(w in ql for w in ["approved", "approval", "success", "successful", "paid"])
    want_declined  = any(w in ql for w in ["declined", "decline", "failed", "failure", "rejected"])
    want_abandoned = any(w in ql for w in ["abandoned", "abandon"])
    want_cancelled = any(w in ql for w in ["cancelled", "canceled", "cancel"])
    want_pct       = any(w in ql for w in ["percent", "percentage", "rate", "%"])

    statuses_wanted = [want_approved, want_declined, want_abandoned, want_cancelled]
    num_statuses = sum(statuses_wanted)

    # Single-status, no percentage → simple COUNT(o.order_id) query (matches DAX ApprovedOrderCount / DeclinedOrderCount)
    if num_statuses == 1 and not want_pct:
        if want_approved:
            status_id = 2
        elif want_declined:
            status_id = 3
        elif want_abandoned:
            status_id = 1
        else:
            status_id = 4

        return f"""SELECT COUNT(o.order_id) AS transaction_count
FROM webxpay_master.tbl_order o
JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = {status_id}
  {date_filter};""".strip()

    # Multi-status or percentage → full breakdown
    # If none specified, show all
    if num_statuses == 0:
        want_approved = want_declined = want_abandoned = want_cancelled = True

    # Denominator: for approved/declined percentage, PowerBI uses payment_status_id <> 1 (excludes abandoned)
    # For abandoned/cancelled percentage we fall back to total COUNT(order_id)
    only_appdec = (want_approved or want_declined) and not (want_abandoned or want_cancelled)
    pct_denom = (
        "SUM(CASE WHEN o.payment_status_id <> 1 THEN 1 ELSE 0 END)"
        if only_appdec and want_pct
        else "COUNT(o.order_id)"
    )

    select_parts = [
        "COUNT(o.order_id) AS total_transactions",
        "SUM(CASE WHEN o.payment_status_id <> 1 THEN 1 ELSE 0 END) AS app_dec_total",
    ]

    if want_approved:
        select_parts.append(
            "SUM(CASE WHEN o.payment_status_id = 2 THEN 1 ELSE 0 END) AS approved_count"
        )
        if want_pct:
            select_parts.append(
                f"ROUND(SUM(CASE WHEN o.payment_status_id = 2 THEN 1 ELSE 0 END) * 100.0 / NULLIF({pct_denom}, 0), 2) AS approved_pct"
            )

    if want_declined:
        select_parts.append(
            "SUM(CASE WHEN o.payment_status_id = 3 THEN 1 ELSE 0 END) AS declined_count"
        )
        if want_pct:
            select_parts.append(
                f"ROUND(SUM(CASE WHEN o.payment_status_id = 3 THEN 1 ELSE 0 END) * 100.0 / NULLIF({pct_denom}, 0), 2) AS declined_pct"
            )

    if want_abandoned:
        select_parts.append(
            "SUM(CASE WHEN o.payment_status_id = 1 THEN 1 ELSE 0 END) AS abandoned_count"
        )
        if want_pct:
            select_parts.append(
                "ROUND(SUM(CASE WHEN o.payment_status_id = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(o.order_id), 0), 2) AS abandoned_pct"
            )

    if want_cancelled:
        select_parts.append(
            "SUM(CASE WHEN o.payment_status_id = 4 THEN 1 ELSE 0 END) AS cancelled_count"
        )
        if want_pct:
            select_parts.append(
                "ROUND(SUM(CASE WHEN o.payment_status_id = 4 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(o.order_id), 0), 2) AS cancelled_pct"
            )

    select_clause = ",\n    ".join(select_parts)

    return f"""SELECT
    {select_clause}
FROM webxpay_master.tbl_order o
JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
WHERE 1=1
  {date_filter};""".strip()


def build_txn_status_timeseries_sql(question: str, ds: str, de: str, grain: str) -> str:
    """
    Time-series version of build_txn_status_sql.
    Groups by day / week / month with the same status breakdown logic.
    """
    ql = question.lower()

    date_filter = ""
    if ds and de:
        date_filter = (
            f"AND p.date_time_transaction >= '{ds}' "
            f"AND p.date_time_transaction < '{de}'"
        )

    # Time bucket expression
    # Note: 'day', 'week', 'month' are reserved in MySQL — use prefixed aliases
    if grain == "day":
        bucket_expr = "DATE(p.date_time_transaction)"
        bucket_alias = "txn_day"
    elif grain == "week":
        bucket_expr = "DATE_FORMAT(p.date_time_transaction, '%x-W%v')"
        bucket_alias = "txn_week"
    else:  # month
        bucket_expr = "DATE_FORMAT(p.date_time_transaction, '%Y-%m')"
        bucket_alias = "txn_month"

    want_approved  = any(w in ql for w in ["approved", "approval", "success", "successful", "paid"])
    want_declined  = any(w in ql for w in ["declined", "decline", "failed", "failure", "rejected"])
    want_abandoned = any(w in ql for w in ["abandoned", "abandon"])
    want_cancelled = any(w in ql for w in ["cancelled", "canceled", "cancel"])
    want_pct       = any(w in ql for w in ["percent", "percentage", "rate", "%"])

    if not any([want_approved, want_declined, want_abandoned, want_cancelled]):
        want_approved = want_declined = want_abandoned = want_cancelled = True

    only_appdec = (want_approved or want_declined) and not (want_abandoned or want_cancelled)
    pct_denom = (
        "SUM(CASE WHEN o.payment_status_id <> 1 THEN 1 ELSE 0 END)"
        if only_appdec and want_pct
        else "COUNT(o.order_id)"
    )

    select_parts = [
        f"{bucket_expr} AS {bucket_alias}",
        "COUNT(o.order_id) AS total_transactions",
        "SUM(CASE WHEN o.payment_status_id <> 1 THEN 1 ELSE 0 END) AS app_dec_total",
    ]

    if want_approved:
        select_parts.append(
            "SUM(CASE WHEN o.payment_status_id = 2 THEN 1 ELSE 0 END) AS approved_count"
        )
        if want_pct:
            select_parts.append(
                f"ROUND(SUM(CASE WHEN o.payment_status_id = 2 THEN 1 ELSE 0 END) * 100.0 / NULLIF({pct_denom}, 0), 2) AS approved_pct"
            )

    if want_declined:
        select_parts.append(
            "SUM(CASE WHEN o.payment_status_id = 3 THEN 1 ELSE 0 END) AS declined_count"
        )
        if want_pct:
            select_parts.append(
                f"ROUND(SUM(CASE WHEN o.payment_status_id = 3 THEN 1 ELSE 0 END) * 100.0 / NULLIF({pct_denom}, 0), 2) AS declined_pct"
            )

    if want_abandoned:
        select_parts.append(
            "SUM(CASE WHEN o.payment_status_id = 1 THEN 1 ELSE 0 END) AS abandoned_count"
        )
        if want_pct:
            select_parts.append(
                f"ROUND(SUM(CASE WHEN o.payment_status_id = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(o.order_id), 0), 2) AS abandoned_pct"
            )

    if want_cancelled:
        select_parts.append(
            "SUM(CASE WHEN o.payment_status_id = 4 THEN 1 ELSE 0 END) AS cancelled_count"
        )
        if want_pct:
            select_parts.append(
                f"ROUND(SUM(CASE WHEN o.payment_status_id = 4 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(o.order_id), 0), 2) AS cancelled_pct"
            )

    select_clause = ",\n    ".join(select_parts)

    return f"""SELECT
    {select_clause}
FROM webxpay_master.tbl_order o
JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
WHERE 1=1
  {date_filter}
GROUP BY {bucket_expr}
ORDER BY {bucket_expr};""".strip()


def build_business_sql(question: str, intent: dict) -> str | None:
    """
    Hard-coded domain rules for WEBXPAY, for common patterns where
    we do NOT want GPT to guess.
    """
    ql = question.lower()
    ds = intent["date_start"]
    de = intent["date_end"]

    date_filter = ""
    if ds and de:
        date_filter = (
            f"AND p.date_time_transaction >= '{ds}' "
            f"AND p.date_time_transaction < '{de}'"
        )

    # ---------- 0) IPG TRANSACTION STATUS (approved/declined/abandoned/cancelled) ----------
    _status_words = {"approved","approval","success","successful","paid",
                     "declined","decline","failed","failure","rejected",
                     "abandoned","abandon","cancelled","canceled","cancel"}
    if (any(w in ql for w in _status_words)
            and "pos" not in ql):
        return build_txn_status_sql(question, ds, de)

    # ---------- 1) TOTAL BANK MDR ("bank rate") IN LKR ----------
    if (
        "bank rate" in ql
        or "bank mdr" in ql
        or ("bank" in ql and "mdr" in ql)
    ):
        currency_ids = detect_currency_ids(question)
        currency_filter = ""
        if currency_ids:
            ids_str = ",".join(f"'{c}'" for c in currency_ids)
            currency_filter = f"AND o.processing_currency_id IN ({ids_str})"

        fx = """
            SELECT er.buying_rate
            FROM tbl_exchange_rate er
            WHERE er.currency_id = o.processing_currency_id
              AND er.date <= DATE(p.date_time_transaction)
            ORDER BY er.date DESC 
            LIMIT 1
        """

        base_lkr = f"""
            CASE
                WHEN o.processing_currency_id = '5' THEN o.total_amount
                ELSE
                    CASE 
                        WHEN o.exchange_rate IS NOT NULL 
                             AND o.exchange_rate NOT LIKE '' 
                             AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
                        THEN o.total_amount * o.exchange_rate
                        ELSE o.total_amount * ({fx})
                    END
            END
        """

        bank_component = """
            CASE 
                WHEN o.order_type_id = 3 
                    THEN (CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4)) + COALESCE(opg.parent_gateway_rate,0))
                ELSE CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4))
            END
        """

        bank_mdr_expr = f"({base_lkr}) * (({bank_component}) / 100.0)"

        sql = f"""
SELECT
    SUM({bank_mdr_expr}) AS total_bank_mdr_lkr
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
LEFT JOIN tbl_order_parent_gateway opg ON opg.order_id = o.order_id
WHERE o.payment_status_id = 2
  {currency_filter}
  {date_filter};
""".strip()
        return sql

    # ---- USD to LKR value (GMV + LKR) ----
    usd_to_lkr_pattern = (
        re.search(r"usd\s+to\s+lkr", ql)
        or re.search(r"usd\s+in\s+lkr", ql)
        or "usd value in lkr" in ql
        or "value of usd in lkr" in ql
    )

    if usd_to_lkr_pattern and "revenue" not in ql:
        fx = """
            SELECT er.buying_rate 
            FROM tbl_exchange_rate er
            WHERE er.currency_id = o.processing_currency_id
              AND er.date <= DATE(p.date_time_transaction)
            ORDER BY er.date DESC 
            LIMIT 1
        """

        # USD transactions converted to LKR
        usd_to_lkr_expr = f"""
            CASE
                WHEN o.processing_currency_id = '2' THEN
                    CASE 
                        WHEN o.exchange_rate IS NOT NULL 
                             AND o.exchange_rate NOT LIKE '' 
                             AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
                        THEN o.total_amount * o.exchange_rate
                        ELSE o.total_amount * ({fx})
                    END
                ELSE 0
            END
        """

        # Native LKR GMV
        lkr_expr = """
            CASE
                WHEN o.processing_currency_id = '5' THEN o.total_amount
                ELSE 0
            END
        """

        sql = f"""
SELECT
    SUM({usd_to_lkr_expr}) AS total_usd_to_lkr,
    SUM({lkr_expr}) AS total_lkr_value,
    SUM({usd_to_lkr_expr}) + SUM({lkr_expr}) AS total_all_value_lkr
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND o.processing_currency_id IN ('2','5')
  {date_filter};
"""
        return sql.strip()

  # ---- Non-transacting merchants ----
    _non_txn_kw = ["non transacting", "non-transacting", "not transacting",
                   "no transaction", "zero transaction", "haven't transacted",
                   "not transacted", "no transact", "dormant merchant",
                   "inactive merchant", "zero transact"]
    _is_non_txn = (
        any(k in ql for k in _non_txn_kw)
        or ("merchant" in ql and ("non" in ql or "not" in ql or "zero" in ql or "no " in ql) and "transact" in ql)
    )
    if _is_non_txn:
        ds = intent.get("date_start")
        de = intent.get("date_end")
        if not ds or not de:
            ds, de = get_period_from_question(question)
        today_str = datetime.now().strftime("%Y-%m-%d")
        ipg_date = ""
        pos_date = ""
        if ds and de:
            effective_de = de if de < today_str else today_str
            ipg_date = f"AND p.date_time_transaction >= '{ds}' AND p.date_time_transaction < '{effective_de}'"
            pos_date = f"AND t.transaction_date >= '{ds}' AND t.transaction_date < '{effective_de}'"

        # ---- POS-only non-transacting ----
        is_pos_channel = "pos" in ql or "point of sale" in ql
        is_ipg_channel = "ipg" in ql or "online" in ql or "gateway" in ql

        if is_pos_channel and not is_ipg_channel:
            return f"""
SELECT
    s.doing_business_name AS merchant_name,
    s.store_id
FROM webxpay_master.tbl_store s
INNER JOIN (
    SELECT DISTINCT store_id FROM webxpay_master.tbl_pos_store_bank_mid WHERE is_active = 1
) pos_enroll ON pos_enroll.store_id = s.store_id
WHERE s.free_trail = 0
  AND s.is_active = 1
  AND NOT EXISTS (
      SELECT 1
      FROM (
          SELECT CONCAT(t.invoice_no,'|',t.auth_code,'|',t.rrn,'|',t.terminal_id,'|',t.terminal_sn) AS pair_key
          FROM webxpay_master.tbl_pos_transactions t
          WHERE t.store_id = s.store_id
            AND t.ipg_provider_id IN (5, 6)
            AND t.currency = 'LKR'
            AND LOWER(TRIM(COALESCE(t.txn_type,''))) NOT IN ('void_sale','void-sale','void_amex','void-amex')
            {pos_date}
          GROUP BY pair_key
      ) _valid
  )
ORDER BY s.doing_business_name;
""".strip()

        # ---- IPG-only non-transacting ----
        if is_ipg_channel and not is_pos_channel:
            return f"""
SELECT
    s.doing_business_name AS merchant_name,
    s.store_id
FROM webxpay_master.tbl_store s
INNER JOIN (
    SELECT DISTINCT store_id FROM webxpay_master.tbl_store_payment_gateway_2 WHERE is_active = 1
) ipg_enroll ON ipg_enroll.store_id = s.store_id
WHERE s.free_trail = 0
  AND s.is_active = 1
  AND NOT EXISTS (
      SELECT 1
      FROM webxpay_master.tbl_order o
      JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
      WHERE o.store_id = s.store_id
        AND o.payment_status_id = 2
        {ipg_date}
  )
ORDER BY s.doing_business_name;
""".strip()

        # ---- Both channels (default) ----
        return f"""
SELECT
    s.doing_business_name AS merchant_name,
    s.store_id
FROM webxpay_master.tbl_store s
WHERE s.free_trail = 0
  AND s.is_active = 1
  AND NOT EXISTS (
      SELECT 1
      FROM webxpay_master.tbl_order o
      JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
      WHERE o.store_id = s.store_id
        AND o.payment_status_id = 2
        {ipg_date}
  )
  AND NOT EXISTS (
      SELECT 1
      FROM (
          SELECT CONCAT(t.invoice_no,'|',t.auth_code,'|',t.rrn,'|',t.terminal_id,'|',t.terminal_sn) AS pair_key
          FROM webxpay_master.tbl_pos_transactions t
          WHERE t.store_id = s.store_id
            AND t.ipg_provider_id IN (5, 6)
            AND t.currency = 'LKR'
            AND LOWER(TRIM(COALESCE(t.txn_type,''))) NOT IN ('void_sale','void-sale','void_amex','void-amex')
            {pos_date}
          GROUP BY pair_key
      ) _valid
  )
ORDER BY s.doing_business_name;
""".strip()

  # ---- Merchant activity bucket analysis ----
    _bucket_kw = ["bucket", "activity bucket", "active day", "active days",
                  "activity analysis", "bucket analysis", "interval", "10-day", "10 day"]
    if any(k in ql for k in _bucket_kw) or ("merchant" in ql and "bucket" in ql):
        ds = intent.get("date_start")
        de = intent.get("date_end")
        if not ds or not de:
            ds, de = get_period_from_question(question)
        # Default: past 90 days
        if not ds or not de:
            _today = datetime.now().date()
            ds = (_today - timedelta(days=90)).strftime("%Y-%m-%d")
            de = _today.strftime("%Y-%m-%d")
        return f"""
SELECT
    COALESCE(c.description, 'Uncategorized') AS merchant_category,
    CASE
        WHEN active_days BETWEEN  1 AND 10 THEN '01-10 days'
        WHEN active_days BETWEEN 11 AND 20 THEN '11-20 days'
        WHEN active_days BETWEEN 21 AND 30 THEN '21-30 days'
        WHEN active_days BETWEEN 31 AND 40 THEN '31-40 days'
        WHEN active_days BETWEEN 41 AND 50 THEN '41-50 days'
        WHEN active_days BETWEEN 51 AND 60 THEN '51-60 days'
        WHEN active_days BETWEEN 61 AND 70 THEN '61-70 days'
        WHEN active_days BETWEEN 71 AND 80 THEN '71-80 days'
        WHEN active_days BETWEEN 81 AND 90 THEN '81-90 days'
        ELSE '91+ days'
    END AS activity_bucket,
    COUNT(*) AS merchant_count
FROM (
    SELECT
        o.store_id,
        s.category_code_id,
        COUNT(DISTINCT DATE(p.date_time_transaction)) AS active_days
    FROM webxpay_master.tbl_order o
    JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
    JOIN webxpay_master.tbl_store s ON s.store_id = o.store_id
    WHERE o.payment_status_id = 2
      AND p.date_time_transaction >= '{ds}'
      AND p.date_time_transaction < '{de}'
    GROUP BY o.store_id, s.category_code_id
) mad
LEFT JOIN webxpay_master.tbl_category_code c ON c.category_code_id = mad.category_code_id
GROUP BY merchant_category, activity_bucket
ORDER BY merchant_category, activity_bucket;
""".strip()

  # ---- Active merchants (POS/IPG/BOTH/TOTAL) ----
    if "merchant" in ql and "active" in ql:
        ds = intent.get("date_start")
        de = intent.get("date_end")

        # if user gave no period, fall back to existing default period logic
        if not ds or not de:
            ds, de = get_period_from_question(question)

        channel_hint = None
        if "ipg" in ql or "online" in ql or "gateway" in ql:
            channel_hint = "ipg"
        elif "pos" in ql or "point of sale" in ql:
            channel_hint = "pos"

        return build_active_merchants_sql(ds, de, channel=channel_hint)


# ---- Total merchants (NOT explicitly active) ----
    if "merchant" in ql and ("total number" in ql or "number of" in ql or "count" in ql) and "active" not in ql:
        return """
    SELECT COUNT(*) AS total_merchants
    FROM tbl_store;
    """.strip()


    # ---- Total number of transactions for EACH merchant (grouped) ----
    if "transaction" in ql and "merchant" in ql and _has_merchant_grouping(question):
        return f"""
SELECT 
    s.store_id,
    s.doing_business_name,
    COUNT(*) AS total_transactions
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
JOIN tbl_store s ON s.store_id = o.store_id
WHERE o.payment_status_id = 2
  {date_filter}
GROUP BY s.store_id, s.doing_business_name
ORDER BY total_transactions DESC;
""".strip()

    # ---- Total number of transactions of <merchant> [in date range] ----
    if "transaction" in ql and "merchant" not in ql:
        m = re.search(r"transactions?\s+(?:of|for)\s+([a-zA-Z0-9 &]+)", ql)
        merchant_name = m.group(1).strip() if m else None

        if merchant_name:
            return f"""
SELECT COUNT(*) AS total_transactions
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
JOIN tbl_store s ON s.store_id = o.store_id
WHERE o.payment_status_id = 2
  AND LOWER(s.doing_business_name) LIKE '%{merchant_name.lower()}%'
  {date_filter};
""".strip()

    # No known business rule -> let generic/GPT handle
    return None
def build_active_merchants_sql(ds: str, de: str, channel: str = None) -> str:
    """
    Returns active merchant count as of the period end date.
    channel: "ipg" | "pos" | None (both)

    Business rules:
      - free_trail = 0
      - activation_date = COALESCE(credit_review_approved_date, active_date); NULL allowed
      - activation_date <= cutoff (if present)
      - diactive_date is NULL / '0000-00-00' / > cutoff
      - IPG: store_id in tbl_store_payment_gateway_2
      - POS: store_id in tbl_pos_store_bank_mid

    Uses LEFT JOINs on pre-aggregated subqueries (not correlated EXISTS) for speed.
    Uses DATE columns directly (no STR_TO_DATE needed — columns are DATE type).
    """
    base_where = "s.free_trail = 0 AND s.is_active = 1"

    if channel == "ipg":
        return f"""
SELECT COUNT(DISTINCT s.store_id) AS active_merchants_ipg
FROM webxpay_master.tbl_store s
INNER JOIN (SELECT DISTINCT store_id FROM webxpay_master.tbl_store_payment_gateway_2) g
    ON g.store_id = s.store_id
WHERE {base_where};
""".strip()

    if channel == "pos":
        return f"""
SELECT COUNT(DISTINCT s.store_id) AS active_merchants_pos
FROM webxpay_master.tbl_store s
INNER JOIN (SELECT DISTINCT store_id FROM webxpay_master.tbl_pos_store_bank_mid WHERE is_active = 1) p
    ON p.store_id = s.store_id
WHERE {base_where};
""".strip()

    # Both channels — use LEFT JOINs computed once (not correlated EXISTS per row)
    return f"""
SELECT
    COUNT(DISTINCT CASE WHEN g.store_id IS NOT NULL AND p.store_id IS NULL THEN s.store_id END)
        AS active_merchants_ipg_only,
    COUNT(DISTINCT CASE WHEN p.store_id IS NOT NULL AND g.store_id IS NULL THEN s.store_id END)
        AS active_merchants_pos_only,
    COUNT(DISTINCT CASE WHEN g.store_id IS NOT NULL AND p.store_id IS NOT NULL THEN s.store_id END)
        AS active_merchants_both_ipg_pos,
    COUNT(DISTINCT CASE WHEN g.store_id IS NOT NULL OR p.store_id IS NOT NULL THEN s.store_id END)
        AS active_merchants_total
FROM webxpay_master.tbl_store s
LEFT JOIN (SELECT DISTINCT store_id FROM webxpay_master.tbl_store_payment_gateway_2) g
    ON g.store_id = s.store_id
LEFT JOIN (SELECT DISTINCT store_id FROM webxpay_master.tbl_pos_store_bank_mid WHERE is_active = 1) p
    ON p.store_id = s.store_id
WHERE {base_where};
""".strip()


# =========================================================
# GENERIC SQL VIA GPT
# =========================================================
def extract_sql_from_text(text: str) -> str:
    code_block = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if code_block:
        return code_block.group(1).strip()
    return text.strip()


def build_generic_sql(question: str, schema: str) -> str:
    # Canonical base_lkr expression we want GPT to reuse
    base_lkr_expr = """
CASE
    WHEN o.processing_currency_id = '5' THEN o.total_amount
    ELSE
        CASE 
            WHEN o.exchange_rate IS NOT NULL 
                 AND o.exchange_rate NOT LIKE '' 
                 AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
            THEN o.total_amount * o.exchange_rate
            ELSE o.total_amount * (
                SELECT er.buying_rate 
                FROM tbl_exchange_rate er
                WHERE er.currency_id = o.processing_currency_id
                  AND er.date <= DATE(p.date_time_transaction)
                ORDER BY er.date DESC 
                LIMIT 1
            )
        END
END
"""

    _today = datetime.now().date()
    _current_year = _today.year
    _ytd_cutoff = _today.strftime("%Y-%m-%d")
    wrong_feedback = load_wrong_feedback(question)

    system_prompt = f"""
You are an expert MySQL SQL generator for a payment gateway system.
{(chr(10) + wrong_feedback + chr(10)) if wrong_feedback else ""}

CURRENT DATE: {_today}
IMPORTANT YEAR AWARENESS:
- The current year is {_current_year}. It is NOT complete — data only exists up to {_ytd_cutoff}.
- When the user compares {_current_year} to a prior year, you MUST use the same YTD period for ALL years
  to make it a fair comparison.
  Example for "2025 vs 2026 GMV":
    WHERE YEAR(p.date_time_transaction) IN (2025, 2026)
      AND DATE_FORMAT(p.date_time_transaction, '%m-%d') <= '{_today.strftime('%m-%d')}'
  (This limits both years to Jan 1 – {_today.strftime('%b %d')} for apple-to-apple comparison.)
- Label the current year column or row as "YTD" where appropriate.
- If the user explicitly asks for a "full year" or doesn't compare years, don't add the YTD restriction.

VERY IMPORTANT BUSINESS RULE ABOUT "TOTAL AMOUNT"/GMV:
- When the user asks for a single "total amount", "total value", or "GMV" across multiple currencies
  (or does not specify a single currency), you MUST:
  - Convert all currencies to LKR first, then sum.
  - Use this exact expression as the base LKR GMV per transaction:

    -- base_lkr (GMV in LKR for one transaction)
    {base_lkr_expr}

  - For total GMV/value in LKR, do: SUM(base_lkr) AS total_gmv_lkr (or a similar alias).
  - NEVER simply do SUM(o.total_amount) across mixed currencies.

- Only when the user EXPLICITLY asks for a single-currency GMV (e.g. "LKR GMV only", "USD GMV only"),
  you may restrict to that processing_currency_id and sum o.total_amount directly in that currency.
- WEBXPAY IS THE COMPANY NAME NOT A MERCHANT.

CRITICAL RULES FOR POS (tbl_pos_transactions) — NEVER DEVIATE FROM THESE:

1. The ONLY valid approved-transaction filter for POS is txn_type.
   NEVER use txn_status or payment_status_id on tbl_pos_transactions.

2. The currency column is the TEXT string "LKR", NOT a numeric ID.
   ALWAYS filter: t.currency = 'LKR'
   NEVER use: processing_currency_id = 5  (that column does not exist in tbl_pos_transactions)

3. Only include ipg_provider_id IN (5, 6)  [5=HNB, 6=DFCC].

4. POS GMV uses a composite pair key (P6PairKey = invoice_no|auth_code|rrn|terminal_id|terminal_sn).
   ONE date-filtered pair-lookup join is required (pk_as) used for ALL metrics: GMV, count, DFCC GMV, revenue.

   Composite key expression (on main table alias t):
   CONCAT(COALESCE(TRIM(CAST(t.invoice_no AS CHAR)),''),'|',COALESCE(TRIM(CAST(t.auth_code AS CHAR)),''),'|',COALESCE(TRIM(t.rrn),''),'|',COALESCE(TRIM(CAST(t.terminal_id AS CHAR)),''),'|',COALESCE(TRIM(CAST(t.terminal_sn AS CHAR)),''))

5. All voids contribute NULL (no negative amounts). Only unpaired sales are counted.
   CASE logic for GMV:
     WHEN ipg_provider_id NOT IN (5,6) THEN NULL
     WHEN currency <> 'LKR' OR amount IS NULL THEN NULL
     WHEN txn_norm NOT IN ('sale','amex','','void_sale','void_amex','void-sale','void-amex') THEN NULL
     WHEN pk_as.pair_key IS NOT NULL THEN NULL   -- paired sale/void → eliminate
     WHEN txn_norm IN ('sale','amex') THEN amount -- unpaired sale
     ELSE NULL                                     -- voids: always NULL

6. The date filter on tbl_pos_transactions uses: t.transaction_date >= '...' AND t.transaction_date < '...'
   The date column is transaction_date (DATE type), NOT created_at or time.

Other rules:
- Use ONLY tables/columns from this schema.
- Do NOT invent tables or columns.
- Generate exactly ONE SELECT query (no INSERT/UPDATE/DELETE/DDL).
- Prefer joining tbl_order, tbl_store, tbl_payment when relevant for IPG queries.
- For IPG transaction date, prefer p.date_time_transaction over o.date_added.
- For merchant name, prefer s.doing_business_name over registered_name.
- For successful IPG transactions, use o.payment_status_id = 2.

Schema:
{schema}
"""

    user_prompt = f"""
User question:
{question}

Write a single valid MySQL SELECT query that answers this question.
Follow the business rule about base_lkr for total GMV/total amount/value.
Do not explain, only output the SQL.
"""

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
    )
    raw = resp.choices[0].message.content
    return extract_sql_from_text(raw)


# =========================================================
# === LLM-ASSISTED ROUTING + REFINEMENT LAYER ============
# =========================================================
def build_base_sql_candidates(question: str, intent: dict) -> dict[str, str]:
    """
    Build a map of canonical/base queries keyed by a short name.
    These are the 'golden' queries whose core logic we want to preserve.
    The LLM will choose one of these and modify it, instead of inventing
    MDR/revenue logic from scratch.
    """
    ql = question.lower()
    candidates: dict[str, str] = {}

    # ✅ POS base query candidate
    if intent.get("type") == "pos" or intent.get("channel") == "pos":
        candidates["pos"] = build_pos_sql(intent)

    # Primary metric-based candidates
    if intent["type"] == "revenue":
        candidates["revenue"] = build_revenue_sql(intent)

    if intent["type"] == "mdr":
        candidates["mdr"] = build_mdr_sql(intent)

    if intent["type"] == "gmv":
        candidates["gmv"] = build_gmv_sql(question, intent)

    if intent["type"] == "volume" or (
        intent["type"] == "count" and "transaction" in ql
    ):
        candidates["volume"] = build_volume_sql(question, intent)

    # Business shortcuts (bank MDR, merchants, usd→lkr, etc.)
    domain_sql = build_business_sql(question, intent)
    if domain_sql:
        candidates["business_rule"] = domain_sql

    # Schema inspection requests → no base query; handled by generic later
    return candidates


def refine_sql_with_llm(
    question: str,
    schema: str,
    intent: dict,
    base_sql_by_key: dict[str, str],
) -> str:
    """
    Core LLM step:
    - Choose a base query and minimally refine it, preserving core logic.
    """
    # Prepare base query bundle
    if base_sql_by_key:
        base_sql_blob = "\n\n".join(
            f"-- KEY: {key}\n{sql}" for key, sql in base_sql_by_key.items()
        )
    else:
        base_sql_blob = "NONE"

    wrong_feedback = load_wrong_feedback(question)

    system_prompt = f"""
You are a senior MySQL query engineer for a payment gateway like WEBXPAY.

You will be given:
- The user's natural language question.
- A detected INTENT object from a rules engine.
- Several CANONICAL BASE QUERIES that are known-correct for things like revenue, MDR, volume, GMV, FX, etc.
- The database schema.
{(chr(10) + wrong_feedback + chr(10)) if wrong_feedback else ""}

YOUR JOB:
1. First, deeply understand the user's question.
2. Decide if ONE of the base queries is a good starting point (same metric & joins).
3. If a base query fits:
   - COPY that query and minimally modify it to fully answer the question.
   - YOU MUST PRESERVE the core calculation logic: all CASE/WHEN blocks, FX logic,
     MDR/revenue formulas, and joins. You can:
       - Add or tweak WHERE filters (dates, merchants, currencies, etc.).
       - Add GROUP BY / ORDER BY / LIMIT.
       - Add extra selected columns (e.g., date breakdowns, merchant names).
       - Wrap in subqueries if needed for comparisons (e.g., previous period vs current).
   - DO NOT simplify or delete the MDR/revenue/FX expressions.
4. If NONE of the base queries apply:
   - Write a completely new SELECT based on the schema and question.


General rules:
- Use ONLY tables/columns from the schema.
- Prefer:
  - p.date_time_transaction for transaction date (IPG).
  - t.transaction_date for transaction date (POS).
  - s.doing_business_name for merchant name.
- For successful IPG transactions, use o.payment_status_id = 2.
- Generate EXACTLY ONE MySQL SELECT statement (no DDL, no INSERT/UPDATE/DELETE).
- Do NOT rename tables (e.g., tbl_pos_transactions must remain tbl_pos_transactions).
- WEBXPAY IS THE COMPANY NAME, NOT A MERCHANT OR GATEWAY.

CHANNEL ROUTING — STRICTLY FOLLOW:
- If the user mentions "pos", "dfcc", "hnb", "point of sale" → use ONLY the POS rules below.
- If the user mentions "ipg", "online", "gateway" → use ONLY the IPG base query (tbl_order + tbl_payment).
- If the user does NOT mention a channel → use ONLY the IPG base query. Do NOT add POS.
  (Combined channel queries are handled separately by the overview system.)

CRITICAL POS RULES — only apply when the user explicitly asks about POS:
1. Filter: t.currency = 'LKR' (text column). NEVER use processing_currency_id = 5.
2. Approval filter: LOWER(TRIM(COALESCE(t.txn_type,''))) IN ('sale','amex'). NEVER use txn_status.
3. Only include ipg_provider_id IN (5, 6). Provider 5 = HNB, Provider 6 = DFCC.
4. POS GMV uses a composite pair key: invoice_no|auth_code|rrn|terminal_id|terminal_sn.
   All voids contribute NULL (no negative amounts). Only unpaired sales count.
   Use ONE date-filtered pair-lookup join (pk_as) for ALL metrics: GMV, count, DFCC GMV, revenue.
   CASE logic: paired → NULL, unpaired sale → amount/revenue, void → NULL (always).
5. Revenue formula (DAX: Total_Revenue = DFCC_POS_revenue + Amex_POS_revenue):
   row_rev = (mdr_rate - 1.7)/100 × amount  if narration = 'visa_master'
           = (mdr_rate - 3.0)/100 × amount  if narration = 'amex'
           = NULL otherwise
   sale/amex → +row_rev, void_sale/void_amex → -row_rev, blank txn_type '' → NULL.
   Get narration via LEFT JOIN tbl_pos_store_bank_mid on bank_merchant_mid. No pair-key needed for revenue.
6. POS date column: t.transaction_date (DATE). Filter: >= 'YYYY-MM-DD' AND < 'YYYY-MM-DD'.

Output:
- Return ONLY the final SQL, wrapped in ```sql ... ```.

Schema:
{schema}
"""

    user_prompt = f"""
USER QUESTION:
{question}

DETECTED INTENT (from rules engine):
{intent}

AVAILABLE BASE QUERIES (may be NONE):
{base_sql_blob}
"""

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.15,
    )
    raw = resp.choices[0].message.content or ""
    return extract_sql_from_text(raw)


# =========================================================
# MAIN ENTRY: GENERATE_SQL
# =========================================================
def generate_sql(question: str, schema: str | None = None) -> str:
    if schema is None:
        schema = load_schema()

    intent = analyze_intent(question)

    # ✅ if user asked daily/weekly/monthly, use time series builders
    grain = intent.get("time_grain")  # "day" | "week" | "month" | None
    ds = intent.get("date_start")
    de = intent.get("date_end")
    ql_gen = question.lower()

    # ✅ Merchant onboarding queries
    _onboard_kw = ["onboard", "onboarded", "new merchant", "newly registered",
                   "registered merchant", "joined", "signed up", "added merchant",
                   "new signups", "merchant signup", "merchant registration"]
    if any(k in ql_gen for k in _onboard_kw):
        grain = intent.get("time_grain")
        if grain and ds and de:
            return build_merchant_onboarding_timeseries_sql(question, ds, de, grain)
        return build_merchant_onboarding_sql(question, ds, de)

    # ✅ Merchant type classification (IPG only / POS only / both)
    _mtype_kw = ["ipg only", "pos only", "ipg and pos", "both ipg and pos",
                 "ipg or pos", "which channel", "merchant type", "merchant channel",
                 "ipg merchant", "pos merchant", "merchant with both", "have both",
                 "has both", "using both", "both channel", "both channels"]
    if any(k in ql_gen for k in _mtype_kw) and "transact" not in ql_gen:
        _want_count = any(w in ql_gen for w in ["how many", "count", "total", "number of"])
        # Detect specific filter
        if "ipg only" in ql_gen or ("ipg" in ql_gen and "pos" not in ql_gen and "both" not in ql_gen):
            _ft = "ipg"
        elif "pos only" in ql_gen or ("pos" in ql_gen and "ipg" not in ql_gen and "both" not in ql_gen):
            _ft = "pos"
        elif "both" in ql_gen or ("ipg and pos" in ql_gen) or ("ipg or pos" in ql_gen):
            _ft = "both"
        else:
            _ft = "all"
        return build_merchant_type_count_sql(_ft) if _want_count else build_merchant_type_sql(_ft)

    # ✅ "In depth" / "information" + date range → daily/weekly timeseries
    # Prevents LLM from generating its own wrong GMV formula
    # Grain: ≤31 days → daily, ≤180 days → weekly, ≤730 days → monthly
    _depth_kw = ["in depth", "in-depth", "useful info", "information", "give me data",
                 "tell me about", "detail", "detailed", "breakdown", "deep dive", "analysis"]
    if (any(k in ql_gen for k in _depth_kw)
            and ds and de
            and "pos" not in ql_gen):
        from datetime import date as _date
        try:
            _ds = _date.fromisoformat(ds)
            _de = _date.fromisoformat(de)
            _days = (_de - _ds).days
            if _days <= 31:
                return build_ipg_timeseries_sql(ds, de, "day")
            elif _days <= 180:
                return build_ipg_timeseries_sql(ds, de, "week")
            elif _days <= 730:
                return build_ipg_timeseries_sql(ds, de, "month")
        except Exception:
            pass

    # ✅ IPG transaction status routing (approved/declined/abandoned/cancelled)
    _status_kw = {"approved","approval","success","successful","paid",
                  "declined","decline","failed","failure","rejected",
                  "abandoned","abandon","cancelled","canceled","cancel"}
    ql_gen = question.lower()
    if any(w in ql_gen for w in _status_kw) and "pos" not in ql_gen:
        return build_txn_status_sql(question, ds, de)

    # ✅ Non-transacting / zero-transacting merchants — MUST run before POS routing
    _non_txn_kw = ["non transacting", "non-transacting", "not transacting",
                   "no transaction", "zero transaction", "haven't transacted",
                   "not transacted", "no transact", "dormant merchant",
                   "inactive merchant", "zero transact"]
    _is_non_txn = (
        any(k in ql_gen for k in _non_txn_kw)
        or ("merchant" in ql_gen
            and any(w in ql_gen for w in ["non", "not", "zero", "no "])
            and "transact" in ql_gen)
    )
    if _is_non_txn:
        direct_sql = build_business_sql(question, intent)
        if direct_sql:
            return direct_sql

    # ✅ POS routing
    if intent.get("type") == "pos" or intent.get("channel") == "pos":
        if grain and ds and de:
            return build_pos_timeseries_sql(ds, de, grain)  # gives year_month rows etc.
        intent["question"] = question  # pass question for merchant grouping detection
        return build_pos_sql(intent)  # single aggregated row or merchant-grouped

    # ✅ If NOT POS, but user explicitly asked for time grain, return IPG time series
    # (covers "monthly breakdown for 2025", "weekly trend", etc.)
    if grain and ds and de:
        return build_ipg_timeseries_sql(ds, de, grain)

    # ✅ Merchant activity bucket analysis — return hardcoded SQL directly (LLM generates wrong syntax)
    _bucket_kw = ["bucket", "activity bucket", "active day", "active days",
                  "activity analysis", "bucket analysis", "10-day", "10 day",
                  "interval", "cohort"]
    if any(k in ql_gen for k in _bucket_kw):
        direct_sql = build_business_sql(question, intent)
        if direct_sql:
            return direct_sql

    # ✅ Non-transacting merchants — return hardcoded SQL directly (LLM ignores free_trail)
    _non_txn_kw = ["non transacting", "non-transacting", "not transacting",
                   "no transaction", "zero transaction", "haven't transacted",
                   "not transacted", "no transact", "dormant merchant",
                   "inactive merchant"]
    if (any(k in ql_gen for k in _non_txn_kw)
            or ("merchant" in ql_gen and ("non" in ql_gen or "not" in ql_gen or "zero" in ql_gen)
                and "transact" in ql_gen)):
        direct_sql = build_business_sql(question, intent)
        if direct_sql:
            return direct_sql

    # Schema requests
    if intent["type"] == "schema":
        return build_generic_sql(question, schema)

    base_candidates = build_base_sql_candidates(question, intent)

    if base_candidates:
        return refine_sql_with_llm(
            question=question,
            schema=schema,
            intent=intent,
            base_sql_by_key=base_candidates,
        )

    return build_generic_sql(question, schema)


# =========================================================
# INSIGHT + ANSWER LAYER (OPTIONAL)  **DEEPER ANALYSIS**
# =========================================================
def generate_insights(question: str, sql_result):
    _today = datetime.now().date()
    _current_year = _today.year
    _ytd_label = _today.strftime("%b %d, %Y")

    # Compute actual row count before potentially truncating result for prompt
    _row_count = len(sql_result) if isinstance(sql_result, list) else None
    _row_count_note = f"\nTOTAL ROWS RETURNED: {_row_count} (use this exact number — do NOT guess or infer a different count)" if _row_count is not None else ""

    # Trim large result sets to avoid bloating the prompt — but preserve count above
    _result_for_prompt = sql_result
    if isinstance(sql_result, list) and len(sql_result) > 50:
        _result_for_prompt = sql_result[:50]

    prompt = f"""You are a senior data analyst for WEBXPAY (Sri Lankan payment gateway).
Today: {_today}. Current year {_current_year} is YTD only (data up to {_ytd_label}) — flag this when comparing to prior full years.
{_row_count_note}

COLUMN NAMING RULES — read column names carefully before writing insights:
- Columns starting with `ipg_` are IPG-only (ipg_gmv_lkr, ipg_revenue_lkr, ipg_txn_volume, ipg_mdr_lkr, ipg_volume). NEVER label these "total GMV" — call them "IPG GMV", "IPG revenue", etc.
- Columns starting with `pos_` are POS-only (pos_gmv_lkr, pos_total_revenue_lkr, pos_volume, pos_hnb_revenue_lkr, pos_dfcc_revenue_lkr).
- Columns starting with `combined_` or `total_` cover both channels (combined_gmv_lkr, combined_revenue_lkr, combined_volume). Only use the word "total" or "combined" when these columns exist in the data.
- If only `ipg_*` columns are present, describe everything as IPG figures only.
- If only `pos_*` columns are present, describe everything as POS figures only.
- Use "Revenue" not "profit".

QUESTION: {question}

DATA (showing up to 50 rows — see TOTAL ROWS RETURNED above for full count): {_result_for_prompt}

Write a concise business insight in this structure:
### 1. Executive summary
- 3-5 bullets directly answering the question with key numbers (use thousand separators, Rs for LKR).

### 2. Analysis
- Key drivers, breakdowns, top contributors (top 3-5 if ranked data). Margin = revenue ÷ GMV if both present.

### 3. Trends
- Describe trend if time column exists. If not, state trend analysis not possible.

### 4. Risks & anomalies
- Outliers, nulls, gaps. If none, say data looks consistent.

### 5. Actions
- 3 concise action-oriented bullets for leadership.

Base everything strictly on the data. If something is missing from the data, say so."""

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.25,
        max_tokens=800,
    )
    return resp.choices[0].message.content.strip()


def build_short_answer(question: str, sql_result):
    """Short direct answers for the most common patterns."""
    if not sql_result:
        return "I couldn't find any matching records for that period."

    # If we get a dict of multiple result sets (overview mode), don't try short answer
    if isinstance(sql_result, dict):
        return None

    row = sql_result[0]
    ql = question.lower()

    if "revenue" in ql and ("usd" in ql or "dollar" in ql or "dollars" in ql):
        val = row.get("total_revenue_usd")
        if val is None:
            return "There is no USD revenue for the selected period."
        try:
            v = float(val)
        except (TypeError, ValueError):
            return "I couldn't interpret the USD revenue value."
        return f"Total USD revenue for the selected period is ${v:,.2f}."

    if "value" in ql and ("usd" in ql or "dollar" in ql or "dollars" in ql):
        val = row.get("total_usd_value") or row.get("total_gmv_usd") or row.get("total_gmv")
        if val is None:
            return "There is no USD value for the selected period."
        try:
            v = float(val)
        except (TypeError, ValueError):
            return "I couldn't interpret the USD value."
        return f"Total USD value for the selected period is ${v:,.2f}."

    if "value" in ql and ("lkr" in ql or "rs" in ql or "rupee" in ql or "rupees" in ql):
        val = row.get("total_lkr_value") or row.get("total_gmv_lkr") or row.get("total_gmv")
        if val is None:
            return "There is no LKR value for the selected period."
        try:
            v = float(val)
        except (TypeError, ValueError):
            return "I couldn't interpret the LKR value."
        return f"Total LKR value for the selected period is Rs {v:,.2f}."

    if "volume" in ql and ("usd" in ql or "dollar" in ql or "dollars" in ql):
        vol = row.get("total_usd_volume") or row.get("total_volume")
        if vol is None:
            return "There are no USD transactions for the selected period."
        return f"Total USD transaction volume for the selected period is {int(vol):,}."

    if "volume" in ql and ("lkr" in ql or "rs" in ql or "rupee" in ql or "rupees" in ql):
        vol = row.get("total_lkr_volume") or row.get("total_volume")
        if vol is None:
            return "There are no LKR transactions for the selected period."
        return f"Total LKR transaction volume for the selected period is {int(vol):,}."

    if "volume" in ql and "usd" not in ql and "lkr" not in ql:
        vol = row.get("total_volume")
        if vol is not None:
            return f"Total transaction volume for the selected period is {int(vol):,}."

    if "bank rate" in ql or "bank mdr" in ql or ("bank" in ql and "mdr" in ql):
        val = row.get("total_bank_mdr_lkr")
        if val is None:
            return "There is no bank MDR for the selected period."
        try:
            v = float(val)
        except (TypeError, ValueError):
            return "I couldn't interpret the bank MDR value."
        return f"Total bank MDR for the selected period is Rs {v:,.2f}."

    if "mdr" in ql and "bank" not in ql:
        val = row.get("total_mdr_lkr")
        if val is None:
            return "There is no MDR for the selected period."
        try:
            v = float(val)
        except (TypeError, ValueError):
            return "I couldn't interpret the MDR value."
        return f"Total MDR for the selected period is Rs {v:,.2f}."

    if (
        re.search(r"usd\s+to\s+lkr", ql)
        or "usd value in lkr" in ql
        or "value of usd in lkr" in ql
        or "usd in lkr" in ql
    ):
        val = row.get("total_usd_to_lkr")
        if val is None:
            return "There is no USD-to-LKR value for the selected period."
        try:
            v = float(val)
        except (TypeError, ValueError):
            return "I couldn't interpret the USD-to-LKR value."
        return f"Total USD value converted to LKR for the selected period is Rs {v:,.2f}."

    return None


# =========================================================
# HIGH-LEVEL MODES: OVERVIEW / TRENDS (NO NEW SQL)
# =========================================================
def detect_high_level_mode(question: str) -> str | None:
    ql = question.lower()

    overview_keywords = [
        "useful information",
        "usefull information",
        "useful info",
        "overview",
        "summary",
        "insight",
        "insights",
        "analysis",
        "analytics",
        "performance",
        "how did we do",
        "how did we perform",
        "how did we do in",
        "how did we perform in",
        "overall picture",
        "overall view",
        "big picture",
        "trends",
        "trend for the year",
        "trend in",
        "yearly trend",
        "year trend",
        "pos and ipg",
        "pos or ipg",
        "pos vs ipg",
        "month analysis",
        "analysis for",
        "tell me about",
        "tell me useful information",
        "highlight","highlights",
        "compare","comparison","vs","performance","perform",
        "percentage","share","contribution",
        "margin","take rate","effective mdr","rate",
        "breakdown","split","mix",
        "channel",
        "risk","anomaly","red flag",
        "recommend","recommendation","actions","next steps","what should we do",
        "focus","prioritize","improve",
        "why","explain","reason","merchant contribution",
        "revenue per transaction","WEBXPAY",
    ]
    if any(k in ql for k in overview_keywords):
        return "period_overview"

    return None


# ✅ ADD: detect requested breakdown granularity (daily/weekly/monthly)
def detect_time_granularity(question: str) -> str | None:
    ql = question.lower()

    if any(k in ql for k in ["daily", "day wise", "daywise", "day-wise", "per day", "by day", "each day"]):
        return "day"
    if any(k in ql for k in ["weekly", "week wise", "weekwise", "week-wise", "per week", "by week", "each week"]):
        return "week"
    if any(k in ql for k in ["monthly", "month wise", "monthwise", "month-wise", "per month", "by month", "each month"]):
        return "month"

    return None


def get_period_from_question(question: str):
    ds, de = extract_date_range(question)
    if ds and de:
        return ds, de

    m = re.search(r"\b(20\d{2})\b", question)
    if m:
        year = int(m.group(1))
        return f"{year}-01-01", f"{year+1}-01-01"

    now = datetime.now()
    return f"{now.year}-01-01", f"{now.year+1}-01-01"


# ✅ ADD: IPG time series SQL builder (day/week/month)
def build_ipg_timeseries_sql(ds: str, de: str, grain: str) -> str:
    if grain == "day":
        bucket = "DATE(p.date_time_transaction)"
        alias = "day"
        group_by = bucket
        order_by = bucket
    elif grain == "week":
        # Show week start date (Monday) so label is readable, e.g. "2026-01-05"
        bucket = "DATE_FORMAT(DATE_SUB(DATE(p.date_time_transaction), INTERVAL WEEKDAY(p.date_time_transaction) DAY), '%Y-%m-%d')"
        alias = "week_start"
        group_by = bucket
        order_by = bucket
    else:
        bucket = "DATE_FORMAT(p.date_time_transaction, '%Y-%m')"
        alias = "year_month"
        group_by = bucket
        order_by = bucket

    return f"""
SELECT
  {bucket} AS `{alias}`,
  SUM(CASE WHEN o.payment_status_id = 2 AND o.processing_currency_id IN ('5','2') THEN ROUND(
    CASE
      WHEN o.processing_currency_id = '5' THEN o.total_amount
      ELSE
        CASE
          WHEN o.exchange_rate IS NOT NULL
           AND o.exchange_rate NOT LIKE ''
           AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
          THEN o.total_amount * o.exchange_rate
          ELSE o.total_amount * (
            SELECT er.buying_rate
            FROM webxpay_master.tbl_exchange_rate er
            WHERE er.currency_id = o.processing_currency_id
              AND er.date <= DATE(p.date_time_transaction)
            ORDER BY er.date DESC
            LIMIT 1
          )
        END
    END
  , 2) ELSE 0 END) AS `ipg_gmv_lkr`,

  ROUND(
    SUM(
      CASE WHEN o.payment_status_id = 2 THEN
      (
        CASE
          WHEN o.processing_currency_id = '5' THEN o.total_amount
          ELSE
            CASE
              WHEN o.exchange_rate IS NOT NULL
               AND o.exchange_rate NOT LIKE ''
               AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
              THEN o.total_amount * o.exchange_rate
              ELSE o.total_amount * (
                SELECT er.buying_rate
                FROM webxpay_master.tbl_exchange_rate er
                WHERE er.currency_id = o.processing_currency_id
                  AND er.date <= DATE(p.date_time_transaction)
                ORDER BY er.date DESC
                LIMIT 1
              )
            END
        END
      )
      *
      (
        (
          o.payment_gateway_rate -
          CASE
            WHEN o.order_type_id = 3
              THEN (CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4)) + COALESCE(opg.parent_gateway_rate, 0))
            ELSE CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4))
          END
        ) / 100.0
      )
      ELSE 0 END
    ), 2
  ) AS `ipg_revenue_lkr`,

  COUNT(o.order_id) AS `ipg_volume`,
  COUNT(DISTINCT o.store_id) AS `unique_merchants`,
  SUM(CASE WHEN o.payment_status_id = 2 THEN 1 ELSE 0 END) AS `approved_count`,
  SUM(CASE WHEN o.payment_status_id = 3 THEN 1 ELSE 0 END) AS `declined_count`,
  SUM(CASE WHEN o.payment_status_id = 1 THEN 1 ELSE 0 END) AS `abandoned_count`,
  ROUND(SUM(CASE WHEN o.payment_status_id = 2 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN o.payment_status_id <> 1 THEN 1 ELSE 0 END), 0), 2) AS `approved_pct`,
  ROUND(SUM(CASE WHEN o.payment_status_id = 3 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN o.payment_status_id <> 1 THEN 1 ELSE 0 END), 0), 2) AS `declined_pct`,

  SUM(
    CASE WHEN o.payment_status_id = 2 THEN
    ROUND(
      (
        CASE
          WHEN o.processing_currency_id = '5' THEN o.total_amount
          ELSE
            CASE
              WHEN o.exchange_rate IS NOT NULL
               AND o.exchange_rate NOT LIKE ''
               AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
              THEN o.total_amount * o.exchange_rate
              ELSE o.total_amount * (
                SELECT er.buying_rate
                FROM webxpay_master.tbl_exchange_rate er
                WHERE er.currency_id = o.processing_currency_id
                  AND er.date <= DATE(p.date_time_transaction)
                ORDER BY er.date DESC
                LIMIT 1
              )
            END
        END
      )
      * (CAST(o.payment_gateway_rate AS DECIMAL(10,4)) / 100.0),
      2
    )
    ELSE 0 END
  ) AS `ipg_mdr_lkr`

FROM webxpay_master.tbl_order o
JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
LEFT JOIN webxpay_master.tbl_order_parent_gateway opg ON opg.order_id = o.order_id
WHERE p.date_time_transaction >= '{ds}'
  AND p.date_time_transaction <  '{de}'
GROUP BY {group_by}
ORDER BY {order_by} ASC;
""".strip()


def build_merchant_onboarding_sql(question: str, ds: str = None, de: str = None) -> str:
    """
    Merchant onboarding count / list.
    Onboard date = COALESCE(credit_review_approved_date, date_registered).
    Detects IPG-only, POS-only, or both from question.
    ds/de: YYYY-MM-DD strings (exclusive end). If None, uses last 3 months.
    """
    ql = question.lower()

    # Specific merchant name lookup: "[Merchant Name] onboarded date"
    _generic_starts = {'what', 'when', 'who', 'is', 'was', 'the', 'a', 'an',
                       'get', 'show', 'find', 'give', 'me', 'for', 'how', 'list', 'tell'}
    _merchant_name_re = re.compile(
        r'^(.+?)\s+(?:onboard(?:ed)?|registr\w*|sign\s*up)\s*(?:date)?',
        re.IGNORECASE
    )
    _m = _merchant_name_re.match(question.strip())
    if _m:
        candidate = _m.group(1).strip()
        if candidate.lower() not in _generic_starts and len(candidate) > 4:
            safe_name = candidate.replace("'", "''")
            return f"""
SELECT
    s.store_id,
    s.doing_business_name,
    COALESCE(s.credit_review_approved_date, s.date_registered) AS onboard_date,
    CASE
        WHEN s.credit_review_approved_date IS NOT NULL THEN 'credit_review_approved_date'
        ELSE 'date_registered'
    END AS date_source
FROM webxpay_master.tbl_store s
WHERE s.doing_business_name LIKE '%{safe_name}%'
  AND s.is_active = 1
ORDER BY onboard_date DESC;
""".strip()

    # Date range
    if ds and de:
        date_filter = f"COALESCE(s.credit_review_approved_date, s.date_registered) >= '{ds}' AND COALESCE(s.credit_review_approved_date, s.date_registered) < '{de}'"
    else:
        date_filter = "COALESCE(s.credit_review_approved_date, s.date_registered) >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH) AND COALESCE(s.credit_review_approved_date, s.date_registered) < CURDATE()"

    # Detect channel
    is_pos = "pos" in ql
    is_ipg = "ipg" in ql or (not is_pos)

    # Channel existence join
    if is_pos and not is_ipg:
        channel_join = """
JOIN (
    SELECT DISTINCT store_id FROM webxpay_master.tbl_pos_store_bank_mid WHERE is_active = 1
) ch ON ch.store_id = s.store_id"""
    elif is_ipg and not is_pos:
        channel_join = """
JOIN (
    SELECT DISTINCT store_id FROM webxpay_master.tbl_store_payment_gateway_2 WHERE is_active = 1
) ch ON ch.store_id = s.store_id"""
    else:
        # Both — union of both channels
        channel_join = """
JOIN (
    SELECT DISTINCT store_id FROM webxpay_master.tbl_store_payment_gateway_2 WHERE is_active = 1
    UNION
    SELECT DISTINCT store_id FROM webxpay_master.tbl_pos_store_bank_mid WHERE is_active = 1
) ch ON ch.store_id = s.store_id"""

    # Detect if user wants a list or just a count
    want_list = any(w in ql for w in ["list", "show", "who", "which", "name", "details"])

    if want_list:
        return f"""
SELECT
    s.store_id,
    s.doing_business_name,
    COALESCE(s.credit_review_approved_date, s.date_registered) AS onboard_date,
    CASE
        WHEN s.credit_review_approved_date IS NOT NULL THEN 'credit_review_approved_date'
        ELSE 'date_registered'
    END AS date_source
FROM webxpay_master.tbl_store s{channel_join}
WHERE {date_filter}
  AND s.free_trail = 0
  AND s.is_active = 1
ORDER BY onboard_date DESC;
""".strip()
    else:
        return f"""
SELECT
    COUNT(DISTINCT s.store_id) AS merchants_onboarded
FROM webxpay_master.tbl_store s{channel_join}
WHERE {date_filter}
  AND s.free_trail = 0
  AND s.is_active = 1;
""".strip()


def build_merchant_onboarding_timeseries_sql(question: str, ds: str, de: str, grain: str) -> str:
    """
    Monthly/weekly/daily breakdown of merchant onboarding count.
    """
    ql = question.lower()

    if grain == "day":
        bucket = "DATE(COALESCE(s.credit_review_approved_date, s.date_registered))"
        alias = "onboard_day"
    elif grain == "week":
        bucket = "DATE_FORMAT(DATE_SUB(DATE(COALESCE(s.credit_review_approved_date, s.date_registered)), INTERVAL WEEKDAY(COALESCE(s.credit_review_approved_date, s.date_registered)) DAY), '%Y-%m-%d')"
        alias = "week_start"
    else:
        bucket = "DATE_FORMAT(COALESCE(s.credit_review_approved_date, s.date_registered), '%Y-%m')"
        alias = "onboard_month"

    is_pos = "pos" in ql
    is_ipg = "ipg" in ql or (not is_pos)

    if is_pos and not is_ipg:
        channel_join = """
JOIN (
    SELECT DISTINCT store_id FROM webxpay_master.tbl_pos_store_bank_mid WHERE is_active = 1
) ch ON ch.store_id = s.store_id"""
    elif is_ipg and not is_pos:
        channel_join = """
JOIN (
    SELECT DISTINCT store_id FROM webxpay_master.tbl_store_payment_gateway_2 WHERE is_active = 1
) ch ON ch.store_id = s.store_id"""
    else:
        channel_join = """
JOIN (
    SELECT DISTINCT store_id FROM webxpay_master.tbl_store_payment_gateway_2 WHERE is_active = 1
    UNION
    SELECT DISTINCT store_id FROM webxpay_master.tbl_pos_store_bank_mid WHERE is_active = 1
) ch ON ch.store_id = s.store_id"""

    return f"""
SELECT
    {bucket} AS `{alias}`,
    COUNT(DISTINCT s.store_id) AS merchants_onboarded
FROM webxpay_master.tbl_store s{channel_join}
WHERE COALESCE(s.credit_review_approved_date, s.date_registered) >= '{ds}'
  AND COALESCE(s.credit_review_approved_date, s.date_registered) < '{de}'
  AND s.free_trail = 0
  AND s.is_active = 1
GROUP BY {bucket}
ORDER BY {bucket} ASC;
""".strip()


def build_merchant_type_sql(filter_type: str = "all") -> str:
    """
    Returns active merchants classified as IPG Only / POS Only / IPG and POS.
    filter_type: 'all' | 'ipg' | 'pos' | 'both'
    """
    where_clause = {
        "ipg":  "ipg.store_id IS NOT NULL AND pos.store_id IS NULL",
        "pos":  "pos.store_id IS NOT NULL AND ipg.store_id IS NULL",
        "both": "ipg.store_id IS NOT NULL AND pos.store_id IS NOT NULL",
        "all":  "(ipg.store_id IS NOT NULL OR pos.store_id IS NOT NULL)",
    }.get(filter_type, "(ipg.store_id IS NOT NULL OR pos.store_id IS NOT NULL)")

    return f"""
SELECT
    s.store_id,
    s.doing_business_name,
    CASE
        WHEN ipg.store_id IS NOT NULL AND pos.store_id IS NOT NULL THEN 'IPG and POS'
        WHEN ipg.store_id IS NOT NULL THEN 'IPG Only'
        WHEN pos.store_id IS NOT NULL THEN 'POS Only'
        ELSE 'Neither'
    END AS merchant_type
FROM webxpay_master.tbl_store s
LEFT JOIN (
    SELECT DISTINCT store_id
    FROM webxpay_master.tbl_store_payment_gateway_2
    WHERE is_active = 1
) ipg ON s.store_id = ipg.store_id
LEFT JOIN (
    SELECT DISTINCT store_id
    FROM webxpay_master.tbl_pos_store_bank_mid
    WHERE is_active = 1
) pos ON s.store_id = pos.store_id
WHERE {where_clause}
  AND s.free_trail = 0
  AND s.is_active = 1
ORDER BY merchant_type, s.doing_business_name;
""".strip()


def build_merchant_type_count_sql(filter_type: str = "all") -> str:
    """Returns counts grouped by merchant_type."""
    where_clause = {
        "ipg":  "ipg.store_id IS NOT NULL AND pos.store_id IS NULL",
        "pos":  "pos.store_id IS NOT NULL AND ipg.store_id IS NULL",
        "both": "ipg.store_id IS NOT NULL AND pos.store_id IS NOT NULL",
        "all":  "(ipg.store_id IS NOT NULL OR pos.store_id IS NOT NULL)",
    }.get(filter_type, "(ipg.store_id IS NOT NULL OR pos.store_id IS NOT NULL)")

    return f"""
SELECT
    CASE
        WHEN ipg.store_id IS NOT NULL AND pos.store_id IS NOT NULL THEN 'IPG and POS'
        WHEN ipg.store_id IS NOT NULL THEN 'IPG Only'
        WHEN pos.store_id IS NOT NULL THEN 'POS Only'
        ELSE 'Neither'
    END AS merchant_type,
    COUNT(*) AS merchant_count
FROM webxpay_master.tbl_store s
LEFT JOIN (
    SELECT DISTINCT store_id
    FROM webxpay_master.tbl_store_payment_gateway_2
    WHERE is_active = 1
) ipg ON s.store_id = ipg.store_id
LEFT JOIN (
    SELECT DISTINCT store_id
    FROM webxpay_master.tbl_pos_store_bank_mid
    WHERE is_active = 1
) pos ON s.store_id = pos.store_id
WHERE {where_clause}
  AND s.free_trail = 0
  AND s.is_active = 1
GROUP BY merchant_type
ORDER BY merchant_count DESC;
""".strip()


# ✅ ADD: POS time series SQL builder (day/week/month)
# ✅ ADD: POS time series SQL builder (day/week/month)
def build_pos_timeseries_sql(ds: str, de: str, grain: str) -> str:
    if grain == "day":
        bucket = "DATE(t.transaction_date)"
        alias = "day"
    elif grain == "week":
        bucket = "DATE_FORMAT(DATE_SUB(t.transaction_date, INTERVAL WEEKDAY(t.transaction_date) DAY), '%Y-%m-%d')"
        alias = "week_start"
    else:
        bucket = "DATE_FORMAT(t.transaction_date, '%Y-%m')"
        alias = "year_month"

    pk_inner = """CONCAT(
            COALESCE(TRIM(CAST(invoice_no  AS CHAR)),''),'|',
            COALESCE(TRIM(CAST(auth_code   AS CHAR)),''),'|',
            COALESCE(TRIM(rrn),''),'|',
            COALESCE(TRIM(CAST(terminal_id AS CHAR)),''),'|',
            COALESCE(TRIM(CAST(terminal_sn AS CHAR)),'')
        )"""

    pair_key_t = """CONCAT(
        COALESCE(TRIM(CAST(t.invoice_no  AS CHAR)),''),'|',
        COALESCE(TRIM(CAST(t.auth_code   AS CHAR)),''),'|',
        COALESCE(TRIM(t.rrn),''),'|',
        COALESCE(TRIM(CAST(t.terminal_id AS CHAR)),''),'|',
        COALESCE(TRIM(CAST(t.terminal_sn AS CHAR)),'')
    )"""

    txn_norm = "LOWER(TRIM(COALESCE(t.txn_type,'')))"

    # Revenue: adjusted_amount × (m.mdr_rate − m.cost_rate) / 100
    # sale/amex=+amount, void=-amount; joined via tbl_pos_store_bank_mid on store_id+bank_merchant_mid.
    adj_amt = f"""CASE
            WHEN {txn_norm} IN ('sale','amex')                                   THEN  t.amount
            WHEN {txn_norm} IN ('void_sale','void_amex','void-sale','void-amex') THEN -t.amount
            ELSE 0
        END"""
    row_rev = f"""ROUND(
            ({adj_amt}) * (COALESCE(m.mdr_rate, 0) - COALESCE(m.cost_rate, 0)) / 100.0
        , 2)"""

    return f"""
SELECT
    {bucket} AS `{alias}`,

    -- GMV [DAX: Total Amount — ALLSELECTED pair via pk_as, no negative voids]
    SUM(CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','','void_sale','void_amex','void-sale','void-amex') THEN NULL
        WHEN pk_as.pair_key IS NOT NULL THEN NULL
        WHEN {txn_norm} IN ('sale','amex') THEN t.amount
        ELSE NULL
    END) AS pos_gmv_lkr,

    -- Volume [unpaired sales only — date-filtered pk_as]
    SUM(CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} IN ('sale','amex') THEN
            CASE WHEN pk_as.pair_key IS NOT NULL THEN NULL ELSE 1 END
        ELSE NULL
    END) AS pos_volume,

    -- DFCC Revenue (PVI 6) [adjusted_amount × (mdr_rate - cost_rate) / 100]
    SUM(CASE
        WHEN t.ipg_provider_id <> 6 THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','void_sale','void_amex','void-sale','void-amex') THEN NULL
        ELSE {row_rev}
    END) AS pos_dfcc_revenue_lkr,

    -- HNB Revenue (PVI 5)
    SUM(CASE
        WHEN t.ipg_provider_id <> 5 THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','void_sale','void_amex','void-sale','void-amex') THEN NULL
        ELSE {row_rev}
    END) AS pos_hnb_revenue_lkr,

    -- Total Revenue (PVI 5+6)
    SUM(CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','void_sale','void_amex','void-sale','void-amex') THEN NULL
        ELSE {row_rev}
    END) AS pos_total_revenue_lkr

FROM webxpay_master.tbl_pos_transactions t
LEFT JOIN (
    -- pk_as: date-filtered pair lookup (ALLSELECTED) → for GMV
    SELECT ipg_provider_id, pair_key
    FROM (
        SELECT ipg_provider_id,
               {pk_inner} AS pair_key,
               LOWER(TRIM(COALESCE(txn_type,''))) AS txn_norm
        FROM webxpay_master.tbl_pos_transactions
        WHERE ipg_provider_id IN (5, 6)
          AND transaction_date >= '{ds}' AND transaction_date < '{de}'
    ) _pas
    GROUP BY ipg_provider_id, pair_key
    HAVING SUM(CASE WHEN txn_norm IN ('sale','amex')             THEN 1 ELSE 0 END) > 0
       AND SUM(CASE WHEN txn_norm IN ('','void_sale','void_amex','void-sale','void-amex') THEN 1 ELSE 0 END) > 0
) pk_as
    ON  t.ipg_provider_id = pk_as.ipg_provider_id
    AND {pair_key_t} = pk_as.pair_key
LEFT JOIN webxpay_master.tbl_pos_store_bank_mid m
    ON  m.store_id          = t.store_id
    AND m.bank_merchant_mid = t.bank_merchant_mid
    AND m.is_active         = 1
WHERE t.transaction_date >= '{ds}'
  AND t.transaction_date <  '{de}'
GROUP BY {bucket}
ORDER BY {bucket} ASC;
""".strip()


# ✅ ADD (FIX): a single summary row for UI tables, WITHOUT overwriting the raw_result dict
def build_overview_table_row(overview_result: dict) -> list[dict]:
    try:
        ds = overview_result.get("period", {}).get("date_from")
        de = overview_result.get("period", {}).get("date_to")

        rev0 = (overview_result.get("revenue") or [{}])[0] or {}
        gmv0 = (overview_result.get("gmv") or [{}])[0] or {}
        vol0 = (overview_result.get("volume") or [{}])[0] or {}
        mdr0 = (overview_result.get("mdr") or [{}])[0] or {}
        pos0 = (overview_result.get("pos") or [{}])[0] or {}

        # -----------------------------
        # IPG (online gateway) — explicit
        # -----------------------------
        ipg_revenue_lkr = rev0.get("total_revenue_lkr")
        ipg_gmv_lkr = gmv0.get("total_gmv_lkr") or rev0.get("total_gmv_lkr")
        ipg_txn_volume = vol0.get("total_volume") or vol0.get("txn_volume")
        ipg_mdr_lkr = mdr0.get("total_mdr_lkr")

        # -----------------------------
        # POS (CHANGED: dfcc + hnb, not amex)
        # -----------------------------
        pos_gmv_lkr = pos0.get("GMV_LKR") or pos0.get("pos_gmv_lkr")
        pos_txn_count = pos0.get("Transaction_Count") or pos0.get("pos_volume")
        pos_dfcc_rev = pos0.get("DFCC_POS_revenue") or pos0.get("pos_dfcc_revenue_lkr")
        pos_hnb_rev = pos0.get("HNB_POS_revenue") or pos0.get("pos_hnb_revenue_lkr")
        pos_total_rev = pos0.get("Total_Revenue") or pos0.get("pos_total_revenue_lkr")

        return [{
            "period_from": ds,
            "period_to": de,

            # --- original keys (kept as-is)
            "ipg_gmv_lkr": ipg_gmv_lkr,
            "ipg_revenue_lkr": ipg_revenue_lkr,
            "ipg_mdr_lkr": ipg_mdr_lkr,
            "ipg_txn_volume": ipg_txn_volume,

            # --- POS
            "pos_gmv_lkr": pos_gmv_lkr,
            "pos_txn_count": pos_txn_count,
            "pos_dfcc_rev": pos_dfcc_rev,
            "pos_hnb_rev": pos_hnb_rev,
            "pos_total_rev": pos_total_rev,
        }]
    except Exception:
        return []


def choose_grain_for_overview(question: str, ds: str, de: str) -> str | None:
    """
    In high-level overview mode:
    - If user explicitly asks daily/weekly/monthly => use it
    - Else if they asked analysis/trend/breakdown and the period is big enough => default to monthly
    - Else return None (keep overview as a single row)
    """
    # 1) explicit grain request
    g = detect_time_granularity(question)
    if g:
        return g

    ql = question.lower()

    # 2) if they are in high-level mode and asking analysis/trends/breakdown, assume monthly for long periods
    trend_words = [
        "trend", "trends", "breakdown", "analysis", "analytics",
        "performance", "month", "monthly", "compare", "vs"
    ]
    wants_trend = any(w in ql for w in trend_words)

    if not wants_trend:
        return None

    # if period is >= ~45 days, monthly makes sense
    try:
        d1 = datetime.strptime(ds, "%Y-%m-%d")
        d2 = datetime.strptime(de, "%Y-%m-%d")
        if (d2 - d1).days >= 45:
            return "month"
    except Exception:
        # If date parsing fails, safest default in overview trend requests is monthly
        return "month"

    # short periods: daily is more appropriate
    return "day"


def handle_period_overview(question: str, sql_executor):
    ds, de = get_period_from_question(question)

    # ✅ detect if user requested daily/weekly/monthly breakdown
    grain = choose_grain_for_overview(question, ds, de)


    revenue_intent = {
        "type": "revenue",
        "metric": "revenue",
        "date_start": ds,
        "date_end": de,
        "currency_id": None,
        "currency_name": None,
    }
    revenue_sql = build_revenue_sql(revenue_intent)

    gmv_intent = {
        "type": "gmv",
        "metric": "gmv",
        "date_start": ds,
        "date_end": de,
        "currency_id": None,
        "currency_name": None,
    }
    gmv_sql = build_gmv_sql("", gmv_intent)

    vol_intent = {
        "type": "volume",
        "metric": "count",
        "date_start": ds,
        "date_end": de,
        "currency_id": None,
        "currency_name": None,
    }
    vol_sql = build_volume_sql("", vol_intent)

    mdr_intent = {
        "type": "mdr",
        "metric": "mdr",
        "date_start": ds,
        "date_end": de,
        "currency_id": None,
        "currency_name": None,
    }
    mdr_sql = build_mdr_sql(mdr_intent)

    pos_intent = {
        "type": "pos",
        "channel": "pos",
        "pos_metric": "all",
        "date_start": ds,
        "date_end": de,
        "currency_id": None,
        "currency_name": None,
    }
    pos_sql = build_pos_sql(pos_intent)

    # Run all 5 overview queries in parallel
    with ThreadPoolExecutor(max_workers=5) as _ex:
        _fut_rev = _ex.submit(sql_executor, revenue_sql)
        _fut_gmv = _ex.submit(sql_executor, gmv_sql)
        _fut_vol = _ex.submit(sql_executor, vol_sql)
        _fut_mdr = _ex.submit(sql_executor, mdr_sql)
        _fut_pos = _ex.submit(sql_executor, pos_sql)
        revenue_rows = _fut_rev.result() or [{}]
        gmv_rows     = _fut_gmv.result() or [{}]
        vol_rows     = _fut_vol.result() or [{}]
        mdr_rows     = _fut_mdr.result() or [{}]
        pos_rows     = _fut_pos.result() or [{}]

    overview_result = {
        "period": {"date_from": ds, "date_to": de},
        "revenue": revenue_rows,
        "gmv": gmv_rows,
        "volume": vol_rows,
        "mdr": mdr_rows,
        "pos": pos_rows,
    }

    # ✅ time series breakdown only when user requests day/week/month
    ipg_ts_sql = None
    pos_ts_sql = None
    if grain:
        ipg_ts_sql = build_ipg_timeseries_sql(ds, de, grain)
        pos_ts_sql = build_pos_timeseries_sql(ds, de, grain)

        with ThreadPoolExecutor(max_workers=2) as _ex:
            _fut_ipg_ts = _ex.submit(sql_executor, ipg_ts_sql)
            _fut_pos_ts = _ex.submit(sql_executor, pos_ts_sql)
            ipg_ts_rows = _fut_ipg_ts.result() or []
            pos_ts_rows = _fut_pos_ts.result() or []

        overview_result["timeseries"] = {
            "grain": grain,
            "ipg": ipg_ts_rows,
            "pos": pos_ts_rows,
        }

    insights = generate_insights(
        f"{question} (period {ds} to {de})", overview_result
    )

    sql_block = {
        "revenue": revenue_sql,
        "gmv": gmv_sql,
        "volume": vol_sql,
        "mdr": mdr_sql,
        "pos": pos_sql,
    }

    if grain:
        sql_block["ipg_timeseries"] = ipg_ts_sql
        sql_block["pos_timeseries"] = pos_ts_sql

    # ✅ FIX: Provide a separate UI-friendly single-row table without losing raw_result/timeseries
    table_result = build_overview_table_row(overview_result)


    return {
        "question": question,
        "sql": sql_block,

        # keep the FULL dict so the LLM sees timeseries and can do monthly/weekly/daily trends
        "raw_result": overview_result,

        # UI can show this as the summary row (like your current "Query results" table)
        "table_result": table_result,

        # UI can optionally render this as a chart/table without digging into raw_result
        "timeseries": overview_result.get("timeseries"),

        "answer": insights,
        "insights": insights,
    }

    

# =========================================================
# MULTI-YEAR COMPARISON DETECTION + HANDLER
# =========================================================
def detect_comparison_years(question: str) -> list[int]:
    """Returns sorted list of years if 2+ distinct years are mentioned."""
    years = sorted(set(int(m) for m in re.findall(r'\b(20\d{2})\b', question)))
    return years if len(years) >= 2 else []


def handle_year_comparison(question: str, years: list[int], sql_executor):
    """
    For multi-year GMV queries (e.g. "2025 vs 2026 GMV"):
    - Runs IPG + POS queries PER YEAR
    - Caps ALL years at the same MM-DD as today for a fair YTD comparison
    - Returns one row per year with ipg_gmv, pos_gmv, total_gmv
    """
    today = datetime.now().date()
    current_year = today.year
    ytd_md = today.strftime("%m-%d")   # e.g. "03-23"

    # ── Composite pair key for POS (DAX P6PairKey) ────────────────────────
    pk_inner_yr = """CONCAT(
            COALESCE(TRIM(CAST(invoice_no  AS CHAR)),''),'|',
            COALESCE(TRIM(CAST(auth_code   AS CHAR)),''),'|',
            COALESCE(TRIM(rrn),''),'|',
            COALESCE(TRIM(CAST(terminal_id AS CHAR)),''),'|',
            COALESCE(TRIM(CAST(terminal_sn AS CHAR)),'')
        )"""

    pair_key_t_yr = """CONCAT(
        COALESCE(TRIM(CAST(t.invoice_no  AS CHAR)),''),'|',
        COALESCE(TRIM(CAST(t.auth_code   AS CHAR)),''),'|',
        COALESCE(TRIM(t.rrn),''),'|',
        COALESCE(TRIM(CAST(t.terminal_id AS CHAR)),''),'|',
        COALESCE(TRIM(CAST(t.terminal_sn AS CHAR)),'')
    )"""

    # pk_as_yr template: date-filtered pair lookup (built per year inside loop)
    def make_pk_as_yr(ds_yr: str, de_yr: str) -> str:
        return f"""LEFT JOIN (
    SELECT ipg_provider_id, pair_key
    FROM (
        SELECT ipg_provider_id,
               {pk_inner_yr} AS pair_key,
               LOWER(TRIM(COALESCE(txn_type,''))) AS txn_norm
        FROM webxpay_master.tbl_pos_transactions
        WHERE ipg_provider_id IN (5, 6)
          AND transaction_date >= '{ds_yr}' AND transaction_date < '{de_yr}'
    ) _pas
    GROUP BY ipg_provider_id, pair_key
    HAVING SUM(CASE WHEN txn_norm IN ('sale','amex')             THEN 1 ELSE 0 END) > 0
       AND SUM(CASE WHEN txn_norm IN ('','void_sale','void_amex','void-sale','void-amex') THEN 1 ELSE 0 END) > 0
) pk_as
    ON  t.ipg_provider_id = pk_as.ipg_provider_id
    AND {pair_key_t_yr} = pk_as.pair_key"""

    base_lkr = """\
CASE
    WHEN o.processing_currency_id = '5' THEN o.total_amount
    ELSE
        CASE
            WHEN o.exchange_rate IS NOT NULL
                 AND o.exchange_rate NOT LIKE ''
                 AND o.exchange_rate REGEXP '^[0-9]+(\\.[0-9]+)?$'
            THEN o.total_amount * o.exchange_rate
            ELSE o.total_amount * (
                SELECT er.buying_rate FROM tbl_exchange_rate er
                WHERE er.currency_id = o.processing_currency_id
                  AND er.date <= DATE(p.date_time_transaction)
                ORDER BY er.date DESC LIMIT 1
            )
        END
END"""

    txn_norm_yr = "LOWER(TRIM(COALESCE(t.txn_type,'')))"

    # POS GMV: composite pair key, no negative voids (DAX: Total Amount)
    pos_gmv_expr = f"""SUM(
    CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm_yr} NOT IN ('sale','amex','','void_sale','void_amex','void-sale','void-amex') THEN NULL
        WHEN pk_as.pair_key IS NOT NULL THEN NULL           -- paired → eliminate
        WHEN {txn_norm_yr} IN ('sale','amex') THEN t.amount -- unpaired sale
        ELSE NULL                                            -- voids: NULL
    END
)"""

    result_rows = []
    sql_blocks = {}

    for yr in years:
        ds = f"{yr}-01-01"
        # Cap at same MM-DD as today for fair YTD comparison
        cap_date = datetime.strptime(f"{yr}-{ytd_md}", "%Y-%m-%d").date() + timedelta(days=1)
        de = cap_date.strftime("%Y-%m-%d")

        # IPG GMV
        ipg_sql = f"""
SELECT IFNULL(SUM(ROUND({base_lkr}, 2)), 0) AS ipg_gmv_lkr
FROM tbl_order o
JOIN tbl_payment p ON p.payment_id = o.payment_id
WHERE o.payment_status_id = 2
  AND p.date_time_transaction >= '{ds}'
  AND p.date_time_transaction < '{de}';
""".strip()

        # POS GMV — uses date-filtered pk_as pair lookup
        pos_sql = f"""
SELECT IFNULL({pos_gmv_expr}, 0) AS pos_gmv_lkr
FROM webxpay_master.tbl_pos_transactions t
{make_pk_as_yr(ds, de)}
WHERE t.transaction_date >= '{ds}'
  AND t.transaction_date < '{de}';
""".strip()

        sql_blocks[f"ipg_{yr}"] = ipg_sql
        sql_blocks[f"pos_{yr}"] = pos_sql

        with ThreadPoolExecutor(max_workers=2) as _ex:
            _fut_ipg_yr = _ex.submit(sql_executor, ipg_sql)
            _fut_pos_yr = _ex.submit(sql_executor, pos_sql)
            ipg_r = _fut_ipg_yr.result()
            pos_r = _fut_pos_yr.result()

        ipg_gmv = float(
            (ipg_r[0].get("ipg_gmv_lkr") or 0)
            if isinstance(ipg_r, list) and ipg_r else 0
        )
        pos_gmv = float(
            (pos_r[0].get("pos_gmv_lkr") or 0)
            if isinstance(pos_r, list) and pos_r else 0
        )

        period_label = (
            f"Jan 1 – {today.strftime('%b %d')} (YTD)" if yr == current_year
            else f"Jan 1 – {today.strftime('%b %d')}"
        )

        result_rows.append({
            "year": yr,
            "period": period_label,
            "ipg_gmv_lkr": round(ipg_gmv, 2),
            "pos_gmv_lkr": round(pos_gmv, 2),
            "total_gmv_lkr": round(ipg_gmv + pos_gmv, 2),
        })

    comparison_note = (
        f"All years capped at Jan 1 – {today.strftime('%b %d')} for a fair YTD comparison. "
        f"{current_year} is the current (incomplete) year."
    )
    insights = generate_insights(
        question + f"\n[Note: {comparison_note}]",
        {"rows": result_rows, "note": comparison_note},
    )

    return {
        "question": question,
        "sql": sql_blocks,
        "raw_result": result_rows,
        "answer": insights,
        "insights": insights,
        "response_type": "data_query",
    }


# =========================================================
# CHANNEL DETECTION (ipg / pos / both)
# =========================================================
def detect_channel(question: str) -> str:
    """
    Returns "pos", "ipg", or "both".
    "both" means the user didn't specify a channel → run IPG + POS and merge.
    """
    ql = question.lower()
    has_pos = "pos" in ql or "hnb" in ql or "dfcc" in ql or "point of sale" in ql
    has_ipg = "ipg" in ql or "online gateway" in ql or "internet payment" in ql
    if has_pos and not has_ipg:
        return "pos"
    if has_ipg and not has_pos:
        return "ipg"
    # Neither or both → run both channels
    return "both"


# =========================================================
# TOP N MERCHANTS HANDLER  (IPG + POS merged per merchant)
# =========================================================
_TOP_MERCHANT_RE = re.compile(
    r'\b(top|best|highest|leading|largest|biggest)\s*(\d+)?\s*(merchants?|stores?|clients?|businesses?)\b',
    re.IGNORECASE
)

def handle_top_merchants(question: str, intent: dict, sql_executor):
    """Per-merchant GMV / revenue / volume ranking combining IPG + POS."""
    ds = intent.get("date_start")
    de = intent.get("date_end")
    if not ds or not de:
        ds, de = get_period_from_question(question)

    ql = question.lower()
    if "revenue" in ql or "profit" in ql:
        metric_type = "revenue"
    elif "volume" in ql or "count" in ql or "transactions" in ql or "number of" in ql:
        metric_type = "volume"
    else:
        metric_type = "gmv"

    # Extract N
    n_match = re.search(r'\b(\d+)\b', question)
    limit_n = int(n_match.group(1)) if n_match else 10
    if limit_n > 200:
        limit_n = 10

    ipg_date = ""
    pos_date_cond = ""
    pos_date_filter = ""
    if ds and de:
        ipg_date      = f"AND p.date_time_transaction >= '{ds}' AND p.date_time_transaction < '{de}'"
        pos_date_cond = f"AND transaction_date >= '{ds}' AND transaction_date < '{de}'"
        pos_date_filter = f"AND t.transaction_date >= '{ds}' AND t.transaction_date < '{de}'"

    # ── IPG per-merchant ────────────────────────────────────────────────────
    usd_to_lkr = """CASE WHEN o.payment_status_id = 2 AND o.processing_currency_id IN ('5','2') THEN ROUND(
        CASE
            WHEN o.processing_currency_id = '5' THEN o.total_amount
            ELSE CASE
                WHEN o.exchange_rate IS NOT NULL AND o.exchange_rate NOT LIKE ''
                     AND o.exchange_rate REGEXP '^[0-9]+(\.[0-9]+)?$'
                THEN o.total_amount * o.exchange_rate
                ELSE o.total_amount * (
                    SELECT er.buying_rate FROM webxpay_master.tbl_exchange_rate er
                    WHERE er.currency_id = o.processing_currency_id
                      AND er.date <= DATE(p.date_time_transaction)
                    ORDER BY er.date DESC LIMIT 1
                )
            END
        END
    , 2) ELSE 0 END"""

    if metric_type == "volume":
        ipg_select = "SUM(CASE WHEN o.payment_status_id = 2 THEN 1 ELSE 0 END) AS ipg_volume"
    elif metric_type == "revenue":
        ipg_select = f"""ROUND(SUM(
            CASE WHEN o.payment_status_id = 2 THEN
                ({usd_to_lkr}) * (
                    (o.payment_gateway_rate -
                     CASE WHEN o.order_type_id = 3
                          THEN (CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4)) + COALESCE(opg.parent_gateway_rate, 0))
                          ELSE CAST(o.bank_payment_gateway_rate AS DECIMAL(10,4))
                     END) / 100.0
                )
            ELSE 0 END
        ), 2) AS ipg_revenue_lkr"""
    else:
        ipg_select = f"ROUND(SUM({usd_to_lkr}), 2) AS ipg_gmv_lkr"

    ipg_sql = f"""
SELECT
    o.store_id,
    s.doing_business_name AS merchant_name,
    {ipg_select}
FROM webxpay_master.tbl_order o
JOIN webxpay_master.tbl_payment p ON p.payment_id = o.payment_id
JOIN webxpay_master.tbl_store s ON s.store_id = o.store_id
LEFT JOIN webxpay_master.tbl_order_parent_gateway opg ON opg.order_id = o.order_id
WHERE 1=1 {ipg_date}
GROUP BY o.store_id, s.doing_business_name
""".strip()

    # ── POS per-merchant ────────────────────────────────────────────────────
    pk_inner = """CONCAT(
            COALESCE(TRIM(CAST(invoice_no  AS CHAR)),''),'|',
            COALESCE(TRIM(CAST(auth_code   AS CHAR)),''),'|',
            COALESCE(TRIM(rrn),''),'|',
            COALESCE(TRIM(CAST(terminal_id AS CHAR)),''),'|',
            COALESCE(TRIM(CAST(terminal_sn AS CHAR)),'')
        )"""
    pair_key_t = """CONCAT(
        COALESCE(TRIM(CAST(t.invoice_no  AS CHAR)),''),'|',
        COALESCE(TRIM(CAST(t.auth_code   AS CHAR)),''),'|',
        COALESCE(TRIM(t.rrn),''),'|',
        COALESCE(TRIM(CAST(t.terminal_id AS CHAR)),''),'|',
        COALESCE(TRIM(CAST(t.terminal_sn AS CHAR)),'')
    )"""
    txn_norm = "LOWER(TRIM(COALESCE(t.txn_type,'')))"

    if metric_type == "volume":
        pos_select = f"""SUM(CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} IN ('sale','amex') THEN
            CASE WHEN pk_as.pair_key IS NOT NULL THEN NULL ELSE 1 END
        ELSE NULL
    END) AS pos_volume"""
    elif metric_type == "revenue":
        adj_amt = f"CASE WHEN {txn_norm} IN ('sale','amex') THEN t.amount WHEN {txn_norm} IN ('void_sale','void_amex','void-sale','void-amex') THEN -t.amount ELSE 0 END"
        pos_select = f"""ROUND(SUM(CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','void_sale','void_amex','void-sale','void-amex') THEN NULL
        ELSE ({adj_amt}) * (COALESCE(m.mdr_rate, 0) - COALESCE(m.cost_rate, 0)) / 100.0
    END), 2) AS pos_total_revenue_lkr"""
    else:
        pos_select = f"""ROUND(SUM(CASE
        WHEN t.ipg_provider_id NOT IN (5, 6) THEN NULL
        WHEN t.currency <> 'LKR' OR t.amount IS NULL THEN NULL
        WHEN {txn_norm} NOT IN ('sale','amex','','void_sale','void_amex','void-sale','void-amex') THEN NULL
        WHEN pk_as.pair_key IS NOT NULL THEN NULL
        WHEN {txn_norm} IN ('sale','amex') THEN t.amount
        ELSE NULL
    END), 2) AS pos_gmv_lkr"""

    pos_sql = f"""
SELECT
    t.store_id,
    s.doing_business_name AS merchant_name,
    {pos_select}
FROM webxpay_master.tbl_pos_transactions t
LEFT JOIN (
    SELECT ipg_provider_id, pair_key
    FROM (
        SELECT ipg_provider_id,
               {pk_inner} AS pair_key,
               LOWER(TRIM(COALESCE(txn_type,''))) AS txn_norm
        FROM webxpay_master.tbl_pos_transactions
        WHERE ipg_provider_id IN (5, 6) {pos_date_cond}
    ) _pas
    GROUP BY ipg_provider_id, pair_key
    HAVING SUM(CASE WHEN txn_norm IN ('sale','amex') THEN 1 ELSE 0 END) > 0
       AND SUM(CASE WHEN txn_norm IN ('','void_sale','void_amex','void-sale','void-amex') THEN 1 ELSE 0 END) > 0
) pk_as
    ON  t.ipg_provider_id = pk_as.ipg_provider_id
    AND {pair_key_t} = pk_as.pair_key
LEFT JOIN webxpay_master.tbl_pos_store_bank_mid m
    ON  m.store_id          = t.store_id
    AND m.bank_merchant_mid = t.bank_merchant_mid
    AND m.is_active         = 1
JOIN webxpay_master.tbl_store s ON s.store_id = t.store_id
WHERE t.ipg_provider_id IN (5, 6)
  AND t.currency = 'LKR'
  {pos_date_filter}
GROUP BY t.store_id, s.doing_business_name
""".strip()

    # ── Run in parallel ─────────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=2) as _ex:
        _fut_ipg = _ex.submit(sql_executor, ipg_sql)
        _fut_pos = _ex.submit(sql_executor, pos_sql)
        ipg_result = _fut_ipg.result()
        pos_result = _fut_pos.result()

    if isinstance(ipg_result, dict) and "error" in ipg_result:
        return {"question": question, "sql": {"ipg": ipg_sql, "pos": pos_sql},
                "raw_result": ipg_result,
                "answer": f"**Database error (IPG):** {ipg_result['error']}",
                "insights": f"**Database error (IPG):** {ipg_result['error']}",
                "response_type": "data_query"}
    if isinstance(pos_result, dict) and "error" in pos_result:
        return {"question": question, "sql": {"ipg": ipg_sql, "pos": pos_sql},
                "raw_result": pos_result,
                "answer": f"**Database error (POS):** {pos_result['error']}",
                "insights": f"**Database error (POS):** {pos_result['error']}",
                "response_type": "data_query"}

    # ── Merge by store_id ───────────────────────────────────────────────────
    merged = {}
    for row in (ipg_result if isinstance(ipg_result, list) else []):
        sid = row.get("store_id")
        if sid not in merged:
            merged[sid] = {"store_id": sid, "merchant_name": row.get("merchant_name", "")}
        merged[sid].update(row)
    for row in (pos_result if isinstance(pos_result, list) else []):
        sid = row.get("store_id")
        if sid not in merged:
            merged[sid] = {"store_id": sid, "merchant_name": row.get("merchant_name", "")}
        merged[sid].update(row)

    sort_key = {"volume": "total_volume", "revenue": "total_revenue_lkr"}.get(metric_type, "total_gmv_lkr")
    for r in merged.values():
        if metric_type == "volume":
            r["total_volume"] = int(r.get("ipg_volume") or 0) + int(r.get("pos_volume") or 0)
        elif metric_type == "revenue":
            r["total_revenue_lkr"] = float(r.get("ipg_revenue_lkr") or 0) + float(r.get("pos_total_revenue_lkr") or 0)
        else:
            r["total_gmv_lkr"] = float(r.get("ipg_gmv_lkr") or 0) + float(r.get("pos_gmv_lkr") or 0)

    rows = sorted(merged.values(), key=lambda r: float(r.get(sort_key) or 0), reverse=True)[:limit_n]

    insights = generate_insights(question, rows)
    return {
        "question": question,
        "sql": {"ipg": ipg_sql, "pos": pos_sql},
        "raw_result": rows,
        "answer": insights,
        "insights": insights,
        "response_type": "data_query",
    }


# =========================================================
# COMBINED IPG + POS QUERY HANDLER
# =========================================================
def handle_combined_query(question: str, intent: dict, sql_executor):
    """
    Runs IPG + POS queries in parallel and returns a merged single-row result
    with ipg_gmv_lkr, pos_gmv_lkr, total_gmv_lkr (or volume equivalents).
    Used when user asks for GMV/volume without specifying a channel.
    """
    ds = intent.get("date_start")
    de = intent.get("date_end")
    metric_type = intent.get("type")  # "gmv", "volume", "revenue"

    # ── Run IPG query ───────────────────────────────────────────────────────
    ipg_intent = dict(intent)
    ipg_sql = None
    ipg_row = {}
    try:
        if metric_type == "gmv":
            ipg_sql = build_gmv_sql(question, ipg_intent)
        elif metric_type == "volume":
            ipg_sql = build_volume_sql(question, ipg_intent)
        elif metric_type == "revenue":
            ipg_sql = build_revenue_sql(ipg_intent)
        else:
            ipg_sql = build_gmv_sql(question, ipg_intent)

        # Build POS SQL here so both can run in parallel below
        _pos_metric_map = {"gmv": "gmv", "volume": "count", "revenue": "total_rev"}
        pos_intent = {
            "type": "pos", "channel": "pos",
            "pos_metric": _pos_metric_map.get(metric_type, "gmv"),
            "date_start": ds, "date_end": de,
            "currency_id": None, "currency_name": None,
        }
        pos_sql = build_pos_sql(pos_intent)

        # Run IPG + POS in parallel
        with ThreadPoolExecutor(max_workers=2) as _ex:
            _fut_ipg = _ex.submit(sql_executor, ipg_sql)
            _fut_pos = _ex.submit(sql_executor, pos_sql)
            ipg_result = _fut_ipg.result()
            pos_result = _fut_pos.result()

        if isinstance(ipg_result, dict) and "error" in ipg_result:
            err = ipg_result["error"]
            return {
                "question": question,
                "sql": {"ipg": ipg_sql, "pos": pos_sql},
                "raw_result": ipg_result,
                "answer": f"**Database error (IPG):** {err}",
                "insights": f"**Database error (IPG):** {err}",
                "response_type": "data_query",
            }
        if isinstance(ipg_result, list) and len(ipg_result) > 0:
            ipg_row = ipg_result[0]
    except Exception as e:
        return {
            "question": question,
            "sql": {"ipg": ipg_sql, "pos": None},
            "raw_result": None,
            "answer": f"**Error building IPG query:** {e}",
            "insights": f"**Error building IPG query:** {e}",
            "response_type": "data_query",
        }

    # ── POS result already fetched above in parallel ─────────────────────────
    if isinstance(pos_result, dict) and "error" in pos_result:
        err = pos_result["error"]
        return {
            "question": question,
            "sql": {"ipg": ipg_sql, "pos": pos_sql},
            "raw_result": pos_result,
            "answer": f"**Database error (POS):** {err}",
            "insights": f"**Database error (POS):** {err}",
            "response_type": "data_query",
        }
    pos_row = pos_result[0] if isinstance(pos_result, list) and pos_result else {}

    # ── Merge into one combined row ─────────────────────────────────────────
    combined = {}

    if metric_type == "gmv" or metric_type is None:
        ipg_gmv  = float(ipg_row.get("total_gmv_lkr") or 0)
        pos_gmv  = float(pos_row.get("pos_gmv_lkr")   or 0)
        hnb_gmv  = float(pos_row.get("hnb_gmv_lkr")   or 0)
        dfcc_gmv = float(pos_row.get("dfcc_gmv_lkr")  or 0)
        combined = {
            "ipg_gmv_lkr":   round(ipg_gmv,  2),
            "pos_gmv_lkr":   round(pos_gmv,  2),
            "hnb_gmv_lkr":   round(hnb_gmv,  2),
            "dfcc_gmv_lkr":  round(dfcc_gmv, 2),
            "total_gmv_lkr": round(ipg_gmv + pos_gmv, 2),
        }
    elif metric_type == "volume":
        ipg_vol  = int(ipg_row.get("total_volume") or ipg_row.get("transaction_count") or 0)
        pos_vol  = int(pos_row.get("Transaction_Count") or pos_row.get("pos_volume") or 0)
        hnb_vol  = int(pos_row.get("hnb_volume")  or 0)
        dfcc_vol = int(pos_row.get("dfcc_volume") or 0)
        combined = {
            "ipg_volume":   ipg_vol,
            "pos_volume":   pos_vol,
            "hnb_volume":   hnb_vol,
            "dfcc_volume":  dfcc_vol,
            "total_volume": ipg_vol + pos_vol,
        }
    elif metric_type == "revenue":
        ipg_rev = float(ipg_row.get("total_revenue_lkr") or 0)
        pos_rev = float(pos_row.get("pos_total_revenue_lkr") or 0)
        combined = {
            "ipg_revenue_lkr":         round(ipg_rev, 2),
            "pos_total_revenue_lkr":   round(pos_rev, 2),
            "total_revenue_lkr":       round(ipg_rev + pos_rev, 2),
        }
    else:
        # fallback: merge raw rows
        combined = {**ipg_row, **pos_row}

    result_list = [combined]
    insights = generate_insights(question, {
        "ipg": ipg_row,
        "pos": pos_row,
        "combined": combined,
    })

    return {
        "question": question,
        "sql": {"ipg": ipg_sql, "pos": pos_sql},
        "raw_result": result_list,
        "answer": insights,
        "insights": insights,
        "response_type": "data_query",
    }


# =========================================================
# QUESTION TYPE CLASSIFIER
# =========================================================
def classify_question(question: str) -> dict:
    """
    Pure-Python classifier — no LLM call, instant response.
    Detects greetings and knowledge questions; everything else is a data_query.
    """
    ql = question.lower().strip()

    # ── Greetings ──
    _greeting_exact = {"hi", "hello", "hey", "hiya", "yo", "sup", "thanks",
                       "thank you", "thank you!", "thanks!", "bye", "goodbye"}
    _greeting_starts = ("good morning", "good afternoon", "good evening",
                        "good night", "hi there", "hey there", "hello there")
    if ql in _greeting_exact or ql.startswith(_greeting_starts):
        return {
            "type": "greeting",
            "answer": (
                "Hello! I'm the WEBXPAY Analytics Assistant. I can help you with:\n"
                "- **Revenue, GMV, MDR** metrics for IPG and POS channels\n"
                "- **Transaction volumes** — approved, declined, abandoned, cancelled\n"
                "- **Merchant analysis** — active, non-transacting, top performers\n"
                "- **Time-series breakdowns** — daily, weekly, monthly\n\n"
                "Just ask a question like: *'What was the IPG revenue for March 2025?'*"
            ),
        }

    # ── Knowledge / definitions ──
    # Skip knowledge classification if question clearly wants data from DB
    _data_intent = [
        "count", "how many", "total", "revenue", "gmv", "volume", "transactions",
        "merchants", "onboard", "approved", "declined", "registered", "active",
        "last month", "this month", "last year", "this year", "last week",
        "last 3 month", "last 3 months", "2025", "2026", "2024", "ytd",
        "yesterday", "today", "january", "february", "march", "april", "may",
        "june", "july", "august", "september", "october", "november", "december",
        "pos merchant", "ipg merchant", "show me", "give me", "list",
    ]
    _has_data_intent = any(d in ql for d in _data_intent)

    _knowledge_triggers = [
        "what is ", "what are ", "what does ", "what do ",
        "explain ", "define ", "definition of", "meaning of",
        "how does ", "how do ", "how is ", "how are ",
        "tell me about", "describe ", "difference between",
        "what is mdr", "what is ipg", "what is pos", "what is gmv",
        "what currencies", "which banks", "how webxpay",
        "payment_status_id", "what banks",
    ]
    if not _has_data_intent and any(ql.startswith(t) or t in ql for t in _knowledge_triggers):
        # Answer via LLM only for knowledge (one call, mini model)
        knowledge_prompt = f"""You are a WEBXPAY payment gateway assistant.
Answer this question concisely using bullet points where helpful.

WEBXPAY context:
- Sri Lankan payment gateway
- IPG = Internet Payment Gateway (online card payments via tbl_order)
- POS = Point of Sale (physical terminals via tbl_pos_transactions, DFCC=provider 6, HNB=provider 5)
- MDR = Merchant Discount Rate (fee % charged to merchants)
- GMV = Gross Merchandise Value (total transaction amount processed)
- Revenue = GMV × (merchant_rate − bank_rate) / 100
- Currencies: LKR (id='5'), USD (id='2'), GBP, EUR, AUD
- payment_status_id: 1=Abandoned, 2=Approved, 3=Declined, 4=Cancelled

Question: {question}"""
        try:
            resp = client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[{"role": "user", "content": knowledge_prompt}],
                temperature=0.2,
                max_tokens=400,
            )
            return {"type": "knowledge", "answer": resp.choices[0].message.content.strip()}
        except Exception:
            return {"type": "knowledge", "answer": "I can answer knowledge questions about WEBXPAY. Please try rephrasing."}

    # ── Everything else is a data query ──
    return {"type": "data_query", "answer": None}


# =========================================================
# OPTIONAL END-TO-END HANDLER
# =========================================================
def handle_user_question(question: str, sql_executor):
    """
    sql_executor(sql: str) -> result (e.g., list[dict])

    Returns:
    {
        "question": ...,
        "sql": ...,
        "raw_result": ...,
        "answer": ...,
        "insights": ...
    }
    """
    # ── Step 0: classify the question ──────────────────────────────────────
    classification = classify_question(question)
    q_type = classification.get("type", "data_query")

    # If it's a knowledge or greeting question, answer directly — no SQL needed
    if q_type in ("knowledge", "greeting"):
        answer = classification.get("answer") or "I'm not sure about that. Could you rephrase?"
        return {
            "question": question,
            "sql": None,
            "raw_result": [],
            "answer": answer,
            "insights": answer,
            "response_type": q_type,
        }

    # ── Step 1: multi-year comparison (e.g. "2025 vs 2026 GMV") ──────────
    # Check this BEFORE overview mode so "2025 vs 2026" doesn't get swallowed
    # by "vs" overview keyword.
    comp_years = detect_comparison_years(question)
    intent_check = analyze_intent(question)
    if (comp_years
            and detect_channel(question) != "pos"
            and intent_check.get("type") in ("gmv", "volume", "revenue", "count", "generic")):
        return handle_year_comparison(question, comp_years, sql_executor)

    # ── Step 1b-pre-0a-i: Merchant onboarding queries ──
    ql_huq = question.lower()
    _onboard_kw_h = ["onboard", "onboarded", "new merchant", "newly registered",
                     "registered merchant", "joined", "signed up", "added merchant",
                     "new signups", "merchant signup", "merchant registration"]
    if any(k in ql_huq for k in _onboard_kw_h):
        _schema_ob = load_schema()
        _sql_ob = generate_sql(question, _schema_ob)
        _result_ob = sql_executor(_sql_ob)
        _insights_ob = generate_insights(question, _result_ob)
        return {
            "question": question,
            "sql": _sql_ob,
            "raw_result": _result_ob,
            "answer": _insights_ob,
            "insights": _insights_ob,
            "response_type": "data_query",
        }

    # ── Step 1b-pre-0a: Merchant type queries (IPG only / POS only / both) ──
    ql_huq = question.lower()
    _mtype_kw_h = ["ipg only", "pos only", "ipg and pos", "both ipg and pos",
                   "ipg or pos", "which channel", "merchant type", "merchant channel",
                   "ipg merchant", "pos merchant", "merchant with both", "have both",
                   "has both", "using both", "both channel", "both channels"]
    if any(k in ql_huq for k in _mtype_kw_h) and "transact" not in ql_huq:
        _schema_mt = load_schema()
        _sql_mt = generate_sql(question, _schema_mt)
        _result_mt = sql_executor(_sql_mt)
        _insights_mt = generate_insights(question, _result_mt)
        return {
            "question": question,
            "sql": _sql_mt,
            "raw_result": _result_mt,
            "answer": _insights_mt,
            "insights": _insights_mt,
            "response_type": "data_query",
        }

    # ── Step 1b-pre-0: Analytical / bucket queries — bypass overview entirely ──
    ql_huq = question.lower()
    _analytical_kw = ["bucket", "activity bucket", "active day", "active days",
                      "activity analysis", "bucket analysis", "10-day", "10 day",
                      "interval", "cohort", "segmentation", "segment analysis"]
    if any(k in ql_huq for k in _analytical_kw):
        schema_a = load_schema()
        sql_a = generate_sql(question, schema_a)
        result_a = sql_executor(sql_a)
        insights_a = generate_insights(question, result_a)
        return {
            "question": question,
            "sql": sql_a,
            "raw_result": result_a,
            "answer": insights_a,
            "insights": insights_a,
            "response_type": "data_query",
        }

    # ── Step 1b-pre-1: Non-transacting / zero-transacting merchants ──
    # Must run BEFORE period_overview AND before POS routing.
    ql_huq = question.lower()
    _non_txn_kw_h = ["non transacting", "non-transacting", "not transacting",
                     "no transaction", "zero transaction", "haven't transacted",
                     "not transacted", "no transact", "dormant merchant",
                     "inactive merchant", "zero transact"]
    _is_non_txn_h = (
        any(k in ql_huq for k in _non_txn_kw_h)
        or ("merchant" in ql_huq
            and any(w in ql_huq for w in ["non", "not", "zero", "no "])
            and "transact" in ql_huq)
    )
    if _is_non_txn_h:
        schema_nt = load_schema()
        sql_nt = generate_sql(question, schema_nt)
        result_nt = sql_executor(sql_nt)
        insights_nt = generate_insights(question, result_nt)
        return {
            "question": question,
            "sql": sql_nt,
            "raw_result": result_nt,
            "answer": insights_nt,
            "insights": insights_nt,
            "response_type": "data_query",
        }

    # ── Step 1b-pre: IPG transaction STATUS questions (approved/declined/abandoned/cancelled) ──
    # Must run BEFORE period_overview so status questions don't get swallowed by the dashboard path.
    _status_words = {"approved","approval","success","successful","paid",
                     "declined","decline","failed","failure","rejected",
                     "abandoned","abandon","cancelled","canceled","cancel"}
    if any(w in ql_huq for w in _status_words) and "pos" not in ql_huq:
        intent_s = analyze_intent(question)
        ds_s = intent_s.get("date_start")
        de_s = intent_s.get("date_end")
        grain_s = detect_time_granularity(question)
        if grain_s:
            sql_s = build_txn_status_timeseries_sql(question, ds_s, de_s, grain_s)
        else:
            sql_s = build_txn_status_sql(question, ds_s, de_s)
        result_s = sql_executor(sql_s)
        insights_s = generate_insights(question, result_s)
        return {
            "question": question,
            "sql": sql_s,
            "raw_result": result_s,
            "answer": insights_s,
            "insights": insights_s,
            "response_type": "data_query",
        }

    # ── Step 1b-pre-2: Timeseries — explicit grain + metric → bypass overview ──
    # "Monthly trend for GMV 2025", "weekly revenue 2025", etc.
    # Must run BEFORE detect_high_level_mode which swallows "trend/trends" keywords.
    _ts_grain = intent_check.get("time_grain")  # "day"|"week"|"month"|None
    _ts_ds    = intent_check.get("date_start")
    _ts_de    = intent_check.get("date_end")
    _ts_metric_kw = ["gmv", "revenue", "volume", "transaction", "mdr",
                     "approved", "declined", "trend", "monthly", "weekly", "daily",
                     "daywise", "weekwise", "monthwise"]
    if (_ts_grain and _ts_ds and _ts_de
            and any(k in ql_huq for k in _ts_metric_kw)):
        _ts_channel = detect_channel(question)
        # Build both IPG and POS timeseries then merge them
        _ts_ipg_sql = build_ipg_timeseries_sql(_ts_ds, _ts_de, _ts_grain)
        _ts_pos_sql = build_pos_timeseries_sql(_ts_ds, _ts_de, _ts_grain)
        if _ts_channel == "ipg":
            _ts_sql = _ts_ipg_sql
            _ts_result = sql_executor(_ts_ipg_sql)
            _ts_insights = generate_insights(question, _ts_result)
            return {
                "question": question, "sql": _ts_ipg_sql,
                "raw_result": _ts_result, "answer": _ts_insights,
                "insights": _ts_insights, "response_type": "data_query",
            }
        elif _ts_channel == "pos":
            _ts_result = sql_executor(_ts_pos_sql)
            _ts_insights = generate_insights(question, _ts_result)
            return {
                "question": question, "sql": _ts_pos_sql,
                "raw_result": _ts_result, "answer": _ts_insights,
                "insights": _ts_insights, "response_type": "data_query",
            }
        else:
            # Both channels — run both in parallel and return as timeseries payload
            with ThreadPoolExecutor(max_workers=2) as _ex:
                _fut_ti = _ex.submit(sql_executor, _ts_ipg_sql)
                _fut_tp = _ex.submit(sql_executor, _ts_pos_sql)
                _ts_ipg_rows = _fut_ti.result()
                _ts_pos_rows = _fut_tp.result()
            _ts_timeseries = {
                "grain": _ts_grain,
                "ipg": _ts_ipg_rows if isinstance(_ts_ipg_rows, list) else [],
                "pos": _ts_pos_rows if isinstance(_ts_pos_rows, list) else [],
            }
            # Merge for insights
            _ts_merged = []
            _grain_key = {"day": "day", "week": "week_start", "month": "year_month"}.get(_ts_grain, "day")
            _ts_map = {}
            for r in (_ts_timeseries["ipg"] + _ts_timeseries["pos"]):
                k = r.get(_grain_key)
                if k not in _ts_map:
                    _ts_map[k] = {_grain_key: k}
                _ts_map[k].update(r)
            _ts_merged = sorted(_ts_map.values(), key=lambda x: str(x.get(_grain_key, "")))
            _ts_insights = generate_insights(question, _ts_merged)
            return {
                "question": question,
                "sql": {"ipg": _ts_ipg_sql, "pos": _ts_pos_sql},
                "raw_result": _ts_timeseries,
                "timeseries": _ts_timeseries,
                "answer": _ts_insights,
                "insights": _ts_insights,
                "response_type": "data_query",
            }

    # ── Step 1b: check for overview / trends mode ──────────────────────────
    # Skip this heavy dashboard path if user previously marked a similar question wrong
    mode = detect_high_level_mode(question)
    if mode == "period_overview" and not is_known_wrong(question):
        return handle_period_overview(question, sql_executor)

    # ── Step 1c: top N merchants per-merchant ranking ─────────────────────
    if _TOP_MERCHANT_RE.search(question):
        return handle_top_merchants(question, intent_check, sql_executor)

    # ── Step 1d: single-period combined (IPG + POS) query ──────────────────
    # Always use this path for revenue/gmv/volume questions with no channel specified —
    # it correctly runs IPG + POS separately and sums the result.
    if (detect_channel(question) == "both"
            and intent_check.get("type") in ("gmv", "volume", "revenue", "generic")
            and intent_check.get("channel") != "pos"):
        metric_type = intent_check.get("type")
        has_metric = metric_type in ("gmv", "volume", "revenue")
        if has_metric:
            return handle_combined_query(question, intent_check, sql_executor)

    # ── Step 2: generate SQL and run it ───────────────────────────────────
    # Check correct-answer cache first — skips LLM SQL generation entirely
    cached_sql = get_cached_sql(question)
    if cached_sql:
        result = sql_executor(cached_sql)
        if result and not (isinstance(result, dict) and "error" in result):
            insights = generate_insights(question, result)
            return {
                "question": question,
                "sql": cached_sql,
                "raw_result": result,
                "answer": insights,
                "insights": insights,
                "response_type": "data_query",
            }
        # Cache hit but DB error → fall through to normal generation

    schema = load_schema()
    sql = generate_sql(question, schema)
    result = sql_executor(sql)

    # ── Step 3: if result is empty/error, try generic SQL as fallback ──────
    def _is_error(r):
        return isinstance(r, dict) and "error" in r

    is_empty = (
        result is None
        or (isinstance(result, list) and len(result) == 0)
        or result == "DB ERROR"
        or _is_error(result)
    )
    if is_empty and sql:
        try:
            fallback_sql = build_generic_sql(question, schema)
            fallback_result = sql_executor(fallback_sql)
            if (fallback_result
                    and not _is_error(fallback_result)
                    and fallback_result != "DB ERROR"
                    and isinstance(fallback_result, list)
                    and len(fallback_result) > 0):
                sql = fallback_sql
                result = fallback_result
        except Exception:
            pass

    # If still an error dict, surface the DB error message to the user
    if _is_error(result):
        err_msg = result.get("error", "Unknown database error")
        return {
            "question": question,
            "sql": sql,
            "raw_result": result,
            "answer": f"**Database error:** {err_msg}",
            "insights": f"**Database error:** {err_msg}",
            "response_type": "data_query",
        }

    answer = build_short_answer(question, result)
    insights = None

    if answer is None:
        try:
            insights = generate_insights(question, result)
        except Exception:
            insights = None
        if insights:
            answer = insights
        else:
            answer = "I couldn't generate a detailed insight summary."

    return {
        "question": question,
        "sql": sql,
        "raw_result": result,
        "answer": answer,
        "insights": insights,
        "response_type": "data_query",
    }
