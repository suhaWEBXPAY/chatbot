from flask import Flask, request, jsonify, send_from_directory
from db import run_sql
from gpt_helpers import handle_user_question, build_overview_table_row
import json
import os
from datetime import datetime

# Load schema
try:
    with open("schema.txt", "r") as f:
        SCHEMA = f.read()
except FileNotFoundError:
    print("FATAL ERROR: schema.txt not found.")
    SCHEMA = ""

app = Flask(__name__, static_folder='static')


# -----------------------------
# CHATBOT ENDPOINT
# -----------------------------
@app.route("/ask", methods=["POST"])
def ask():
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()

    if not question:
        return jsonify({"error": "Question is required."}), 400

    try:
        payload = handle_user_question(question, run_sql)

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
            "response_type": payload.get("response_type", "data_query"),
        }), 200

    except Exception as e:
        return jsonify({
            "question": question,
            "sql": None,
            "result": [],
            "raw_result": None,
            "timeseries": None,
            "insights": f"**Error:** {e}",
        }), 200

# -----------------------------
# FEEDBACK ENDPOINT
# -----------------------------
FEEDBACK_FILE = os.path.join(os.path.dirname(__file__), "feedback_log.json")

@app.route("/feedback", methods=["POST"])
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
# SERVE FRONTEND
# -----------------------------
@app.route("/")
def index():
    return send_from_directory('static', 'index.html')


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
