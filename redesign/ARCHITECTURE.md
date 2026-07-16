# WEBXPAY Chatbot — Prompt-Based Redesign

## The one-line idea
Stop *routing to functions with keywords*; start *planning with tools*.
Keep every validated SQL builder you already have — expose them as tools the LLM selects,
instead of a 440-line `if/elif` chain selecting them.

## Pipeline

```
user turn
   │
   ▼
[1 ROUTE]  fast LLM → lane = db | web | hybrid | chit_chat | meta
   │        also rewrites follow-ups into standalone questions
   │
   ├─ chit_chat / meta ─────────────► answer from prompt/history (no tools)
   ├─ web ───────► [web tool] search + cite ──► answer
   ├─ hybrid ────► db branch ⊕ web branch ──► merged answer
   │
   ▼ (db)
[2 PLAN]   LLM → { tool, params(dates resolved), channel, requested_output, assumptions,
   │              must_include_metrics }        ← JSON, no SQL yet
   │
   ▼
[3 SQL]    tool in registry → deterministic builder returns validated SQL
   │        tool == adhoc_sql → LLM writes SQL (only path where LLM touches SQL)
   │
   ▼
[4 VALIDATE]  read-only guard + no SELECT* + table/col exist + auto-LIMIT + timeout hint
   │           reject or auto-fix before execution
   ▼
[5 EXECUTE]   run_sql (read-only pool, MAX_EXECUTION_TIME)
   │
   ▼
[6 GROUND]    compute facts over FULL result set (totals, max/min, coverage)
   │
   ▼
[7 ANSWER]    LLM explains using ONLY rows+facts, matched to requested_output,
              shows date range/channel/assumptions, adds follow-ups
   │
   ▼
[8 LOG + CACHE + FEEDBACK]
```

## What maps to what (migration, not rewrite)

| Old (gpt_helpers.py)                         | New role                                            |
|----------------------------------------------|-----------------------------------------------------|
| `handle_user_question` if/elif chain (L5171) | DELETE → `orchestrator.handle_user_question`        |
| `classify_question` (L4471)                  | REPLACE → ROUTE step (LLM lane)                     |
| `analyze_intent` keyword logic (L584)        | REPLACE → PLAN step (LLM structured)                |
| `llm_extract_query_spec` (L4609)             | ABSORB into PLAN                                     |
| `build_pos_sql`, `build_gmv_sql`, `build_*`  | KEEP → tools in `TOOL_REGISTRY`                     |
| `handle_top_merchants`, `handle_combined_*`  | KEEP → `FULL_HANDLERS`                              |
| `refine_sql_with_llm` (L2497)                | KEEP for `adhoc_sql` only                           |
| `generate_insights` (L2819)                  | REPLACE → ANSWER step (prompt 4)                    |
| `_compute_result_facts` (L2754)              | KEEP → GROUND step                                  |
| `get_cached_sql` / feedback log              | KEEP → wrap around plan (cache on plan hash)        |
| `db._is_read_only_select`                    | KEEP → reused by `validator.validate_sql`           |

## E. Routing logic (lanes)
- **db** (default for internal metrics): merchants, GMV, revenue, MDR, volume, status, POS, IPG,
  currencies, onboarding, comparisons, trends, rankings, "why did X change".
- **web**: regulations, competitors, market/news, external standards/docs, public definitions.
  Must cite sources. Never feed internal numbers to the web tool.
- **hybrid**: internal number + external context → run both, label each source.
- **meta**: about the previous answer → answer from history, no SQL.
- **chit_chat**: greet/thanks.
Rule: internal figures ALWAYS come from the DB; the web lane never produces a company metric.

## F. SQL/DAX validation rules (validator.py)
1. Single read-only SELECT/WITH (reuse `db._is_read_only_select`).
2. No `SELECT *`.
3. Every table/column must exist in `INFORMATION_SCHEMA` (load once into `SCHEMA_TABLES`).
4. Mandatory `LIMIT <= max_rows`, auto-injected for non-scalar queries; cap oversized LIMITs.
5. JOINs must have `ON` (block cartesian products).
6. Ratios must use `NULLIF(denom,0)` (division-by-zero safety).
7. Dates as half-open `>= start AND < end` (no BETWEEN off-by-one).
8. Add `MAX_EXECUTION_TIME` hint so runaway queries self-abort.
9. `-- CANNOT_ANSWER: reason` from the model → surfaced, not executed.
DAX: keep DAX in Power BI as the source of truth; the chatbot translates the *same measure
definitions* into SQL (the semantic layer in the planner prompt mirrors the DAX measures).

## G. Fallback & error handling
- Router fails / low confidence → default lane = db, note assumption.
- Planner returns no tool → `adhoc_sql`.
- Validation blocks → return the problem list, do NOT execute; log for review.
- SQL error dict → surface DB error verbatim in `insights`, keep app 200 (as today).
- Empty result → say "no records for <exact filters/period>", never fabricate.
- adhoc SQL empty → retry once with `build_generic_sql` (existing behavior).
- LLM/network error in ANSWER → fall back to a deterministic table + one-line summary.
- Known-wrong (feedback) match → force `adhoc_sql` re-plan, avoid the cached wrong shape.

## I. Example prompts → expected behavior

| User question | lane | tool | requested_output | Answer shape |
|---|---|---|---|---|
| "Which merchants dropped more than 10% this month?" | db | period_comparison (this vs last month) | ranking | table of merchants with prev, curr, % drop; filter ≥10% |
| "Why did GMV reduce in June?" | db | gmv_drop_diagnosis / period_comparison | explanation | per-merchant movers, top decliners, stated reason from numbers |
| "Compare POS and IPG for May and June." | db | timeseries(both, month) | comparison | 2×2 table (channel×month) + deltas + % |
| "Show failed transactions by provider." | db | txn_status(declined)+gateway_breakdown | detail_rows | per-provider declined counts |
| "Which merchants improved the most?" | db | period_comparison | ranking | top gainers by % |
| "What should management focus on?" | db | combined_summary + top_merchants | recommendation | 3-4 prioritised actions tied to figures |
| "Latest CBSL rules on IPG settlement?" | web | web search | explanation | summary + source links |
| "How does our June GMV compare to SL e-commerce growth?" | hybrid | combined_summary + web | comparison | our number (DB) vs market figure (web, cited) |

## J. Recommendations (speed / accuracy / security)
**Speed**
- Cache on the *plan hash* (tool+params) not raw text → dedupes paraphrases; reuse `query_cache.sqlite3`.
- Keep the pre-aggregated `summary_mart.sqlite3` for period overviews; refresh nightly.
- Run IPG+POS branches in parallel (you already do via ThreadPoolExecutor).
- Router + planner on the fast model, `reasoning_effort="none"`; only ANSWER gets a little temperature.
- Materialize a daily merchant×channel GMV/revenue rollup table → most questions hit one small table.
**Accuracy**
- Semantic layer is the single source of metric definitions (planner prompt) — mirror the DAX measures.
- `must_include_metrics` from the plan → post-check the answer actually reports them.
- Ground every number in `_compute_result_facts`; superlatives from full set, not the 50-row sample.
- Golden-question regression set (from `feedback_log.json`) run on every prompt change.
**Security**
- Read-only DB user at the MySQL grant level (belt-and-suspenders with `_is_read_only_select`).
- Never select raw PAN/email/card columns; add a column denylist in the validator for
  `card_number`, `card_number_first_six`, `card_holder_email`, `card_holder_mobile_number`,
  `secret_key`, `private_key`, `db_password` unless a role explicitly allows it.
- Role-based lanes: analysts get merchant-level; others get aggregates only.
- Log {question, lane, plan, sql, rows, ms, warnings} per turn (orchestrator._log_turn).
- Keep API keys in `.env` (already done); rotate the Gemini key.

## Rollout (low-risk)
1. Land `redesign/` alongside current code (done). No behavior change yet.
2. Add `SCHEMA_TABLES` loader + wire the 3 LLM calls in `orchestrator.py`.
3. Shadow-run: send 10% of `/ask` traffic through `orchestrator.handle_user_question`, compare
   to the legacy path, diff answers, tune prompts.
4. Flip default once the golden set passes; keep legacy path behind a flag for one release.
