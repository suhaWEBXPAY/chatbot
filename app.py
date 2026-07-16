from flask import Flask, request, jsonify, send_from_directory
from auth import init_auth_db, login_required, register_auth_routes
from db import run_sql
from gpt_helpers import handle_user_question, build_overview_table_row
import json
import os
import time
from datetime import datetime

# Load schema
try:
    with open("schema.txt", "r") as f:
        SCHEMA = f.read()
except FileNotFoundError:
    print("FATAL ERROR: schema.txt not found.")
    SCHEMA = ""

app = Flask(__name__, static_folder='static')
app.secret_key = (
    os.getenv("FLASK_SECRET_KEY")
    or os.getenv("SECRET_KEY")
    or "change-me-before-deploying"
)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)
if os.getenv("SESSION_COOKIE_SECURE", "").lower() in {"1", "true", "yes", "on"}:
    app.config["SESSION_COOKIE_SECURE"] = True

init_auth_db()
register_auth_routes(app)


# -----------------------------
# SUMMARY-MART AUTO-REFRESH
# -----------------------------
# Keeps the local pre-computed mart (daily GMV per store + store->RM mapping)
# current so heavy questions (RM rankings, trends) answer instantly instead of
# timing out on live MySQL scans. Disable with MART_AUTO_REFRESH=0.
def _mart_refresh_loop():
    import time as _time
    interval = int(os.getenv("MART_REFRESH_HOURS", "6")) * 3600
    _time.sleep(20)  # let the server finish starting first
    while True:
        try:
            from redesign.pos_summary_mart import refresh
            refresh(days_back=3)  # also refreshes store_dim
        except Exception as e:
            print(f"[mart_refresh] failed: {e}")
        _time.sleep(interval)


if os.getenv("MART_AUTO_REFRESH", "1").lower() not in {"0", "false", "no"}:
    import threading
    threading.Thread(target=_mart_refresh_loop, daemon=True,
                     name="mart-refresh").start()


# -----------------------------
# CHATBOT ENDPOINT
# -----------------------------
@app.route("/ask", methods=["POST"])
@login_required
def ask():
    # Multipart requests carry file uploads (images/documents) next to the
    # question; JSON requests are the normal text-only path.
    uploaded_files = []
    if (request.content_type or "").startswith("multipart/form-data"):
        data = {
            "question": request.form.get("question", ""),
            "engine": request.form.get("engine"),
        }
        try:
            data["history"] = json.loads(request.form.get("history") or "[]")
        except Exception:
            data["history"] = []
        from file_analysis import ALLOWED_EXTENSIONS, MAX_FILES, MAX_FILE_BYTES
        for fs in request.files.getlist("files")[:MAX_FILES]:
            if not fs.filename:
                continue
            ext = fs.filename.rsplit(".", 1)[-1].lower() if "." in fs.filename else ""
            if ext not in ALLOWED_EXTENSIONS:
                return jsonify({"error": f"Unsupported file type: .{ext}"}), 400
            blob = fs.read()
            if len(blob) > MAX_FILE_BYTES:
                return jsonify({"error": f"{fs.filename} is too large (max 10 MB)."}), 400
            uploaded_files.append((fs.filename, blob))
    else:
        data = request.get_json(silent=True) or {}

    question = (data.get("question") or "").strip()

    if not question and not uploaded_files:
        return jsonify({"error": "Question is required."}), 400

    # Conversation history for follow-up resolution (most recent last).
    # Defensive: accept only well-formed {role, content} items.
    history = []
    _raw_hist = data.get("history")
    if isinstance(_raw_hist, list):
        for _m in _raw_hist[-6:]:
            if isinstance(_m, dict) and _m.get("content"):
                history.append({
                    "role": "assistant" if _m.get("role") in ("ai", "assistant") else "user",
                    "content": str(_m.get("content"))[:2000],
                })

    # Engine switch (safe, opt-in). Default = current/legacy engine, so behavior is
    # unchanged unless you turn the new engine on via either:
    #   - env var:  CHATBOT_ENGINE=new
    #   - per request body: {"engine": "new"}  (lets a UI toggle switch per message)
    _use_new = (
        (data.get("engine") == "new")
        or os.getenv("CHATBOT_ENGINE", "").lower() == "new"
    )

    _t_ask = time.monotonic()
    try:
        if uploaded_files:
            # File analysis path — the answer is grounded in the uploaded
            # images/documents, no SQL pipeline involved.
            from file_analysis import analyze_files
            payload = analyze_files(question, uploaded_files, history=history)
        elif _use_new:
            # PURE new-engine mode: no legacy fallback, so any failure is visible
            # (that's what you want while validating the new engine on its own).
            import traceback
            try:
                from redesign.engine import handle_user_question_new
                payload = handle_user_question_new(question, run_sql, history=history)
            except Exception as _new_err:
                _tb = traceback.format_exc()
                print(f"[ask] NEW ENGINE ERROR:\n{_tb}")
                payload = {
                    "question": question,
                    "sql": None,
                    "raw_result": [],
                    "answer": f"**New-engine error:** {_new_err}\n\n```\n{_tb[-1500:]}\n```",
                    "insights": f"**New-engine error:** {_new_err}",
                    "response_type": "error",
                    "engine": "new",
                }
        else:
            payload = handle_user_question(question, run_sql, history=history)

        print(f"[timing] /ask total {time.monotonic() - _t_ask:.1f}s :: "
              f"{question[:100]!r}", flush=True)

        raw_result = payload.get("raw_result")
        sql_used = payload.get("sql")
        insights_text = payload.get("answer")
        table_result = payload.get("table_result")
        timeseries = payload.get("timeseries")

        # -----------------------------
        # 1) Normalize timeseries so ipg/pos are always lists
        # -----------------------------
        ts_out = None
        if isinstance(timeseries, dict):
            ts_out = {
                "grain": timeseries.get("grain"),
                "ipg": [],
                "pos": [],
                "errors": {}
            }

            ipg_rows = timeseries.get("ipg")
            pos_rows = timeseries.get("pos")

            # if executor returned {"error": "..."} keep it in errors and return []
            if isinstance(ipg_rows, dict) and "error" in ipg_rows:
                ts_out["errors"]["ipg"] = ipg_rows.get("error")
            elif isinstance(ipg_rows, list):
                ts_out["ipg"] = ipg_rows

            if isinstance(pos_rows, dict) and "error" in pos_rows:
                ts_out["errors"]["pos"] = pos_rows.get("error")
            elif isinstance(pos_rows, list):
                ts_out["pos"] = pos_rows

            # if no errors at all, remove errors key (optional)
            if not ts_out["errors"]:
                ts_out.pop("errors", None)

        # -----------------------------
        # 2) result_out must be list[dict]
        #    - overview mode: if timeseries exists => return multi-row
        #    - otherwise => return single-row table_result
        #    - normal mode => raw_result list
        # -----------------------------
        result_out = []

        def _merge_timeseries_rows(ts: dict) -> list[dict]:
            """
            Merge IPG + POS timeseries on the grain key.
            Adds combined_gmv_lkr / combined_revenue_lkr / combined_volume.
            """
            if not isinstance(ts, dict):
                return []

            grain = ts.get("grain")
            ipg_rows = ts.get("ipg") if isinstance(ts.get("ipg"), list) else []
            pos_rows = ts.get("pos") if isinstance(ts.get("pos"), list) else []

            key_map = {"day": "day", "week": "year_week", "month": "year_month"}
            k = key_map.get(grain)
            if not k:
                return []

            merged = {}

            def ensure(key):
                if key not in merged:
                    merged[key] = {k: key}
                return merged[key]

            # Add IPG rows
            for r in ipg_rows:
                key = r.get(k)
                if key is None:
                    continue
                row = ensure(key)
                row.update(r)

            # Add POS rows
            for r in pos_rows:
                key = r.get(k)
                if key is None:
                    continue
                row = ensure(key)
                row.update(r)

            # Compute combined metrics
            out = []
            for key in sorted(merged.keys()):
                r = merged[key]

                ipg_gmv = float(r.get("ipg_gmv_lkr") or 0)
                pos_gmv = float(r.get("pos_gmv_lkr") or 0)

                ipg_rev = float(r.get("ipg_revenue_lkr") or 0)
                pos_rev = float(r.get("pos_total_revenue_lkr") or 0)

                ipg_vol = int(r.get("ipg_volume") or 0)
                pos_vol = int(r.get("pos_volume") or 0)

                r["combined_gmv_lkr"] = ipg_gmv + pos_gmv
                r["combined_revenue_lkr"] = ipg_rev + pos_rev
                r["combined_volume"] = ipg_vol + pos_vol

                out.append(r)

            return out

        if isinstance(raw_result, dict) and "error" in raw_result:
            # DB ERROR — return empty result, surface error in insights
            result_out = []
            if not insights_text:
                insights_text = f"**Database error:** {raw_result['error']}"
        elif isinstance(raw_result, dict):
            # OVERVIEW MODE
            # If we have normalized timeseries with rows => return multi-row
            if isinstance(ts_out, dict) and (
                (isinstance(ts_out.get("ipg"), list) and len(ts_out.get("ipg")) > 0) or
                (isinstance(ts_out.get("pos"), list) and len(ts_out.get("pos")) > 0)
            ):
                result_out = _merge_timeseries_rows(ts_out)

                # fallback: if merge fails for some reason, return the single row
                if not result_out:
                    result_out = table_result if isinstance(table_result, list) else []
            else:
                # No timeseries => return the single summary row
                result_out = table_result if isinstance(table_result, list) else []
        else:
            # NORMAL MODE
            result_out = raw_result if isinstance(raw_result, list) else []

        return jsonify({
            "question": payload.get("question"),
            "sql": sql_used,          # dict or string (frontend can stringify)
            "result": result_out,     # always list[dict]
            "raw_result": raw_result, # unchanged (dict or list)
            "timeseries": ts_out,     # normalized
            "insights": insights_text,
            "chart": payload.get("chart"),   # optional backend-built chart spec
            "response_type": payload.get("response_type", "data_query"),
        }), 200

    except Exception as e:
        _es = str(e)
        if any(t in _es for t in ("503", "UNAVAILABLE", "high demand", "overloaded", "429")):
            _msg = ("The AI engine is temporarily overloaded (high demand on the model "
                    "provider). Please try the same question again in a few seconds.")
        else:
            _msg = f"**Error:** {e}"
        return jsonify({
            "question": question,
            "sql": None,
            "result": [],
            "raw_result": None,
            "timeseries": None,
            "insights": _msg,
            "response_type": "conversation",
        }), 200

# -----------------------------
# FEEDBACK ENDPOINT
# -----------------------------
FEEDBACK_FILE = os.path.join(os.path.dirname(__file__), "feedback_log.json")

@app.route("/feedback", methods=["POST"])
@login_required
def feedback():
    data = request.get_json(silent=True) or {}
    correct   = data.get("correct")        # bool
    question  = (data.get("question") or "").strip()
    sql       = data.get("sql")
    insights  = data.get("insights")

    if not question:
        return jsonify({"error": "question required"}), 400

    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "correct":   correct,
        "question":  question,
        "sql":       sql,
        "insights":  insights,
    }

    # Load existing log, append, save
    log = []
    if os.path.exists(FEEDBACK_FILE):
        try:
            with open(FEEDBACK_FILE, "r") as f:
                log = json.load(f)
        except Exception:
            log = []

    log.append(entry)

    with open(FEEDBACK_FILE, "w") as f:
        json.dump(log, f, indent=2, default=str)

    return jsonify({"status": "saved"}), 200


# -----------------------------
# LEARNED RULES (user-taught corrections)
# -----------------------------
@app.route("/lessons", methods=["GET"])
@login_required
def lessons_list():
    from learning_store import list_lessons
    return jsonify({"lessons": list_lessons()}), 200


@app.route("/lessons/<lesson_id>", methods=["DELETE"])
@login_required
def lessons_delete(lesson_id):
    from learning_store import delete_lesson
    if delete_lesson(lesson_id):
        return jsonify({"status": "deleted"}), 200
    return jsonify({"error": "not found"}), 404


# -----------------------------
# SERVE FRONTEND
# -----------------------------
@app.route("/")
@login_required
def index():
    return send_from_directory('static', 'index.html')


if __name__ == "__main__":
    port = int(os.getenv("PORT") or os.getenv("FLASK_RUN_PORT") or 5000)
    app.run(host="0.0.0.0", port=port, debug=True)
