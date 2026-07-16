import os
import json
import sqlite3
from datetime import datetime
from functools import wraps

from flask import jsonify, redirect, request, send_from_directory, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash


AUTH_DB_PATH = os.getenv(
    "AUTH_DB_PATH",
    os.path.join(os.path.dirname(__file__), "auth_users.sqlite3"),
)


def _utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _connect():
    conn = sqlite3.connect(AUTH_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def init_auth_db():
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                last_login_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_sessions (
                session_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                title TEXT,
                messages_json TEXT NOT NULL DEFAULT '[]',
                client_updated_at INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (session_id, user_id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_sessions_user ON chat_sessions(user_id, updated_at DESC)")


def user_count() -> int:
    with _connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM users").fetchone()
    return int(row["total"] or 0)


def signup_enabled() -> bool:
    return user_count() == 0  # only open for the very first admin account


def _row_to_user(row) -> dict | None:
    if not row:
        return None
    return {
        "id": row["id"],
        "username": row["username"],
        "email": row["email"],
        "role": row["role"],
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
        "last_login_at": row["last_login_at"],
    }


def get_user_by_id(user_id: int) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, username, email, role, is_active, created_at, last_login_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
    return _row_to_user(row)


def get_current_user() -> dict | None:
    user_id = session.get("user_id")
    if not user_id:
        return None
    user = get_user_by_id(user_id)
    if not user or not user["is_active"]:
        session.clear()
        return None
    return user


def _wants_json() -> bool:
    if request.path.startswith(("/api/", "/auth/")):
        return True
    if request.path in {"/ask", "/feedback"}:
        return True
    return request.accept_mimetypes.best == "application/json"


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if get_current_user():
            return view(*args, **kwargs)
        if _wants_json():
            return jsonify({"error": "Authentication required."}), 401
        return redirect(url_for("login_page", next=request.path))

    return wrapped


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        user = get_current_user()
        if not user:
            if _wants_json():
                return jsonify({"error": "Authentication required."}), 401
            return redirect(url_for("login_page", next=request.path))
        if user.get("role") != "admin":
            if _wants_json():
                return jsonify({"error": "Admin access required."}), 403
            return redirect(url_for("index"))
        return view(*args, **kwargs)

    return wrapped


def _request_data() -> dict:
    if request.is_json:
        return request.get_json(silent=True) or {}
    return request.form.to_dict()


def _validate_new_user(data: dict) -> tuple[str | None, str | None, str | None]:
    username = (data.get("username") or "").strip().lower()
    email = (data.get("email") or "").strip().lower() or None
    password = data.get("password") or ""

    if len(username) < 3:
        return None, None, "Username must be at least 3 characters."
    if not username.replace("_", "").replace(".", "").replace("-", "").isalnum():
        return None, None, "Username can only contain letters, numbers, dots, dashes, and underscores."
    if len(password) < 8:
        return None, None, "Password must be at least 8 characters."

    return username, email, None


def create_user(username: str, email: str | None, password: str, role: str = "user") -> dict:
    role = role if role in {"admin", "user"} else "user"
    password_hash = generate_password_hash(password)
    now = _utc_now()

    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO users (username, email, password_hash, role, is_active, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            (username, email, password_hash, role, now),
        )
        user_id = cur.lastrowid

    return get_user_by_id(user_id)


def register_auth_routes(app):
    @app.route("/login")
    def login_page():
        if get_current_user():
            return redirect(url_for("index"))
        return send_from_directory("static", "login.html")

    @app.route("/users")
    @admin_required
    def users_page():
        return send_from_directory("static", "users.html")

    @app.route("/logout", methods=["GET", "POST"])
    def logout():
        session.clear()
        if request.method == "POST" or _wants_json():
            return jsonify({"ok": True})
        return redirect(url_for("login_page"))

    @app.route("/auth/status")
    def auth_status():
        return jsonify(
            {
                "setup_required": user_count() == 0,
                "signup_enabled": signup_enabled(),
                "user": get_current_user(),
            }
        )

    @app.route("/auth/login", methods=["POST"])
    def auth_login():
        data = _request_data()
        username = (data.get("username") or "").strip().lower()
        password = data.get("password") or ""

        with _connect() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE username = ?",
                (username,),
            ).fetchone()

        if not row or not row["is_active"] or not check_password_hash(row["password_hash"], password):
            return jsonify({"error": "Invalid username or password."}), 401

        now = _utc_now()
        with _connect() as conn:
            conn.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (now, row["id"]))

        session.clear()
        session.permanent = True
        session["user_id"] = row["id"]
        session["username"] = row["username"]
        session["role"] = row["role"]

        return jsonify({"ok": True, "user": get_user_by_id(row["id"])})

    @app.route("/auth/register", methods=["POST"])
    def auth_register():
        first_user = user_count() == 0
        current_user = get_current_user()
        can_register = first_user or signup_enabled() or (current_user and current_user.get("role") == "admin")
        if not can_register:
            return jsonify({"error": "Signup is disabled. Ask an admin to create your account."}), 403

        data = _request_data()
        username, email, error = _validate_new_user(data)
        if error:
            return jsonify({"error": error}), 400

        requested_role = (data.get("role") or "user").strip().lower()
        role = "admin" if first_user else "user"
        if current_user and current_user.get("role") == "admin" and requested_role in {"admin", "user"}:
            role = requested_role

        try:
            user = create_user(username, email, data.get("password") or "", role)
        except sqlite3.IntegrityError:
            return jsonify({"error": "That username is already taken."}), 409

        if first_user:
            session.clear()
            session.permanent = True
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]

        return jsonify({"ok": True, "user": user})

    @app.route("/api/me")
    @login_required
    def api_me():
        return jsonify({"user": get_current_user()})

    @app.route("/api/users", methods=["GET", "POST"])
    @admin_required
    def api_users():
        if request.method == "POST":
            data = _request_data()
            username, email, error = _validate_new_user(data)
            if error:
                return jsonify({"error": error}), 400
            role = (data.get("role") or "user").strip().lower()
            try:
                user = create_user(username, email, data.get("password") or "", role)
            except sqlite3.IntegrityError:
                return jsonify({"error": "That username is already taken."}), 409
            return jsonify({"ok": True, "user": user}), 201

        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT id, username, email, role, is_active, created_at, last_login_at
                FROM users
                ORDER BY created_at DESC
                """
            ).fetchall()
        return jsonify({"users": [_row_to_user(row) for row in rows]})

    @app.route("/api/users/<int:user_id>/status", methods=["PATCH"])
    @admin_required
    def api_user_status(user_id: int):
        current_user = get_current_user()
        if current_user and current_user["id"] == user_id:
            return jsonify({"error": "You cannot deactivate your own account."}), 400

        data = _request_data()
        is_active = 1 if bool(data.get("is_active")) else 0
        with _connect() as conn:
            cur = conn.execute(
                "UPDATE users SET is_active = ? WHERE id = ?",
                (is_active, user_id),
            )
        if cur.rowcount == 0:
            return jsonify({"error": "User not found."}), 404
        return jsonify({"ok": True, "user": get_user_by_id(user_id)})

    @app.route("/api/chat-sessions", methods=["GET"])
    @login_required
    def api_chat_sessions():
        user = get_current_user()
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT session_id, title, messages_json, client_updated_at, created_at, updated_at
                FROM chat_sessions
                WHERE user_id = ?
                ORDER BY updated_at DESC
                """,
                (user["id"],),
            ).fetchall()

        sessions = []
        for row in rows:
            try:
                messages = json.loads(row["messages_json"] or "[]")
            except json.JSONDecodeError:
                messages = []
            sessions.append(
                {
                    "id": row["session_id"],
                    "title": row["title"],
                    "messages": messages,
                    "updatedAt": row["client_updated_at"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )
        return jsonify({"sessions": sessions})

    @app.route("/api/chat-sessions/<session_id>", methods=["PUT", "POST"])
    @login_required
    def api_save_chat_session(session_id: str):
        user = get_current_user()
        data = _request_data()
        title = (data.get("title") or "").strip() or None
        messages = data.get("messages") if isinstance(data.get("messages"), list) else []
        client_updated_at = int(data.get("updatedAt") or 0)
        now = _utc_now()
        messages_json = json.dumps(messages, separators=(",", ":"), default=str)

        with _connect() as conn:
            existing = conn.execute(
                """
                SELECT client_updated_at
                FROM chat_sessions
                WHERE session_id = ? AND user_id = ?
                """,
                (session_id, user["id"]),
            ).fetchone()

            if existing and int(existing["client_updated_at"] or 0) > client_updated_at:
                return jsonify({"ok": True, "ignored": True})

            conn.execute(
                """
                INSERT INTO chat_sessions (
                    session_id, user_id, title, messages_json,
                    client_updated_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, user_id) DO UPDATE SET
                    title = excluded.title,
                    messages_json = excluded.messages_json,
                    client_updated_at = excluded.client_updated_at,
                    updated_at = excluded.updated_at
                """,
                (session_id, user["id"], title, messages_json, client_updated_at, now, now),
            )

        return jsonify({"ok": True})

    @app.route("/api/chat-sessions/<session_id>", methods=["DELETE"])
    @login_required
    def api_delete_chat_session(session_id: str):
        user = get_current_user()
        with _connect() as conn:
            conn.execute(
                "DELETE FROM chat_sessions WHERE session_id = ? AND user_id = ?",
                (session_id, user["id"]),
            )
        return jsonify({"ok": True})
