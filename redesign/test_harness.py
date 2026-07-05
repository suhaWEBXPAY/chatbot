"""
REGRESSION HARNESS — runs many questions through the new engine's decision pipeline
(route -> plan -> build_sql -> validate) and prints a report, so you don't have to test
questions one at a time in the UI.

It does NOT execute against the database (fast, safe, no timeouts). It checks the part that
has actually been breaking: routing, tool selection, date resolution, and SQL validity.

Run:  python redesign/test_harness.py
      python redesign/test_harness.py --only pos      (filter by keyword)
      python redesign/test_harness.py --flags          (show only rows needing attention)

Each row is flagged:
  OK      routed to a validated builder + passed validation
  ADHOC   fell to the AI SQL writer (review: is there a builder that should cover this?)
  FAIL    validation rejected the SQL, or the pipeline errored
  DATE?   no year found / a suspicious past year in the SQL
"""
import sys, os, re, json, traceback
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from demo import route, plan, build_sql
from validator import validate_sql
from schema_loader import get_schema_tables

# ── Test set: one representative question per capability, grouped by category ──
TESTS = {
    "GMV / revenue / volume": [
        "what was the IPG revenue for March 2025",
        "total GMV last month",
        "POS revenue for 2026",
        "how many transactions in June 2026",
        "what is the loss for month of december 2025",
        "whats our revenue for the month of february",
    ],
    "channel split / type": [
        "how many merchants use IPG only",
        "merchants with both IPG and POS",
        "pos only merchants",
    ],
    "status": [
        "approved vs declined IPG transactions for 2025 monthwise",
        "show failed transactions by provider",
        "abandoned payments last month",
    ],
    "trend / timeseries": [
        "monthly revenue overview for 2025",
        "weekly GMV trend for 2026",
        "daily transactions from 2026-06-01 to 2026-06-15",
    ],
    "merchant activity": [
        "pos non transacting merchants 2026",
        "who are the non transacting merchants for 2026 january",
        "merchant in pos with no transaction for january 2026",
        "active transacting merchants for may 2026",
        "how many merchants were onboarded last month",
    ],
    "ranking": [
        "top 10 merchants by GMV last month",
        "which merchants improved the most in june vs may",
    ],
    "comparison / diagnosis": [
        "compare POS and IPG revenue for May and June 2026",
        "why did GMV drop in june compared to may",
        "merchants who dropped more than 10% in june compared to may by mcc",
        "2025 vs 2026 GMV",
    ],
    "multi-step / adhoc-expected": [
        "did the merchants onboarded last month do any transactions",
        "merchants who signed up but never paid",
    ],
    "non-db": [
        "what are the latest CBSL regulations on payment gateways",
        "hello",
    ],
    "real logged questions": [
        "give me information from time period january 2nd to 5th 2026 give in depth",
        "can you provide me the IPG transactions approved and declined percentage for 2025 monthwise",
    ],
}

CURRENT_YEAR = date.today().year


def classify(question):
    try:
        r = route(question)
        lane = r.get("lane", "?")
        q = r.get("standalone_question") or question
        if lane in ("web", "hybrid", "chit_chat", "meta"):
            return {"lane": lane, "tool": "-", "flag": "OK", "notes": lane}
        pl = plan(q)
        tool = pl.get("tool", "?")
        # Mirror the engine: these tools are handled end-to-end by validated legacy
        # handlers (multi-query + own grounding), NOT via build_sql. Testing build_sql
        # for them is misleading, so mark them handled here (as the engine does).
        if tool in ("gmv_drop_diagnosis", "merchant_movers"):
            return {"lane": lane, "tool": tool, "flag": "OK", "notes": "full handler"}
        sql = build_sql(q, pl)
        ok, safe_sql, problems = validate_sql(sql, get_schema_tables(),
                                              max_rows=pl.get("params", {}).get("limit", 1000))
        years = sorted(set(re.findall(r"'(20\d\d)-", safe_sql)))
        flag = "OK"
        notes = []
        if not ok:
            flag = "FAIL"; notes.append(str(problems))
        elif tool == "adhoc_sql":
            flag = "ADHOC"; notes.append("AI-written SQL")
        # date sanity: db questions should reference a plausible year
        if lane == "db" and years and all(int(y) < CURRENT_YEAR - 1 for y in years):
            flag = "DATE?" if flag == "OK" else flag
            notes.append(f"years={years}")
        elif lane == "db" and not years and "onboard" not in q.lower():
            notes.append("no explicit year in SQL")
        return {"lane": lane, "tool": tool, "flag": flag, "notes": "; ".join(notes)}
    except Exception as e:
        return {"lane": "?", "tool": "?", "flag": "FAIL", "notes": f"{e} | {traceback.format_exc().splitlines()[-1]}"}


def main():
    only = None
    flags_only = "--flags" in sys.argv
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1].lower()

    counts = {"OK": 0, "ADHOC": 0, "FAIL": 0, "DATE?": 0}
    print(f"\n{'FLAG':6} {'LANE':6} {'TOOL':26} QUESTION")
    print("-" * 100)
    for cat, questions in TESTS.items():
        shown = [q for q in questions if not only or only in q.lower()]
        if not shown:
            continue
        print(f"\n# {cat}")
        for q in shown:
            res = classify(q)
            counts[res["flag"]] = counts.get(res["flag"], 0) + 1
            if flags_only and res["flag"] == "OK":
                continue
            line = f"{res['flag']:6} {res['lane']:6} {res['tool']:26} {q[:52]}"
            if res["notes"]:
                line += f"   <- {res['notes'][:60]}"
            print(line)

    total = sum(counts.values())
    print("\n" + "=" * 100)
    print(f"SUMMARY  total={total}  OK={counts['OK']}  ADHOC={counts['ADHOC']}  "
          f"FAIL={counts['FAIL']}  DATE?={counts['DATE?']}")
    print("FAIL = must fix. ADHOC = review whether a validated builder should cover it. "
          "DATE? = wrong/again year.")


if __name__ == "__main__":
    main()
