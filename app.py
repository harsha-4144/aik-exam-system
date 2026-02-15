import os
from flask import Flask, render_template, request, redirect, session, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import subprocess
import resource
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from functools import wraps
from werkzeug.security import check_password_hash
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "exam-secret-key-change-this")

# ---------- DATABASE ----------
db_pool = None
db_pool_lock = threading.Lock()

schema_initialized = False
schema_lock = threading.Lock()


def init_db_pool():
    global db_pool
    if db_pool is not None:
        return

    with db_pool_lock:
        if db_pool is not None:
            return

        try:
            db_pool = SimpleConnectionPool(
                int(os.getenv("DB_POOL_MINCONN", "1")),
                int(os.getenv("DB_POOL_MAXCONN", "10")),
                os.getenv("DATABASE_URL"),
            )
        except Exception as e:
            print(f"DB pool init failed, falling back to direct connections: {e}")
            db_pool = None


def ensure_exam_schema():
    global schema_initialized
    if schema_initialized:
        return

    with schema_lock:
        if schema_initialized:
            return

        conn = get_db(raw=True)
        if not conn:
            return

        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS exam_sessions (
                    id SERIAL PRIMARY KEY,
                    session_name TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    duration_minutes INTEGER NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS question_ids TEXT DEFAULT '';
                ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS is_paused BOOLEAN DEFAULT FALSE;
                ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS paused_at TIMESTAMP;
                ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS participant_limit INTEGER DEFAULT 0;
                ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS auto_start_when_all_ready BOOLEAN DEFAULT FALSE;

                CREATE TABLE IF NOT EXISTS exam_participants (
                    id SERIAL PRIMARY KEY,
                    session_id INTEGER NOT NULL REFERENCES exam_sessions(id) ON DELETE CASCADE,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    ready_status BOOLEAN DEFAULT FALSE,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_exam_participants_session_user
                    ON exam_participants(session_id, user_id);

                CREATE TABLE IF NOT EXISTS exam_progress (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    question_id INTEGER NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
                    status TEXT DEFAULT 'in_progress',
                    verdict TEXT,
                    score INTEGER DEFAULT 0,
                    time_spent INTEGER DEFAULT 0
                );
                ALTER TABLE exam_progress ADD COLUMN IF NOT EXISTS session_id INTEGER REFERENCES exam_sessions(id) ON DELETE CASCADE;
                ALTER TABLE exam_progress ADD COLUMN IF NOT EXISTS verdict TEXT;
                CREATE UNIQUE INDEX IF NOT EXISTS ux_exam_progress_session_user_question
                    ON exam_progress(session_id, user_id, question_id);

                CREATE TABLE IF NOT EXISTS submission_test_results (
                    id SERIAL PRIMARY KEY,
                    submission_id INTEGER NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
                    test_case_id INTEGER,
                    case_type TEXT NOT NULL,
                    input_data TEXT,
                    expected_output TEXT,
                    actual_output TEXT,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS ix_submission_test_results_submission
                    ON submission_test_results(submission_id);

                CREATE TABLE IF NOT EXISTS exam_settings (
                    setting_key TEXT PRIMARY KEY,
                    setting_value TEXT NOT NULL
                );

                INSERT INTO exam_settings(setting_key, setting_value)
                VALUES ('questions_hidden', '0')
                ON CONFLICT (setting_key) DO NOTHING;

                ALTER TABLE questions ADD COLUMN IF NOT EXISTS display_order INTEGER;
                UPDATE questions
                SET display_order = id
                WHERE display_order IS NULL;
                """
            )
            conn.commit()
            schema_initialized = True
        except Exception as e:
            print(f"Schema initialization error: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            close_db(conn)


def close_db(conn):
    if not conn:
        return
    try:
        if db_pool is not None:
            db_pool.putconn(conn)
        else:
            close_db(conn)
    except Exception:
        pass

def get_db(raw=False):
    """Get PostgreSQL database connection"""
    if raw:
        try:
            if db_pool is not None:
                return db_pool.getconn()
            return psycopg2.connect(os.getenv("DATABASE_URL"))
        except Exception as e:
            print(f"Database connection error: {e}")
            return None
    return get_db_safe()

def get_db_safe():
    init_db_pool()
    ensure_exam_schema()
    try:
        if db_pool is not None:
            return db_pool.getconn()
        return psycopg2.connect(os.getenv("DATABASE_URL"))
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def db():
    return get_db_safe()

def is_logged_in():
    return "user_id" in session

def is_admin():
    return session.get("username") == "admin"

# ---------- EXAM SESSION HELPERS ----------
api_rate_limits = {}
api_rate_lock = threading.Lock()


def rate_limited(bucket_key, max_hits=30, window_seconds=60):
    now = time.time()
    with api_rate_lock:
        hits = api_rate_limits.get(bucket_key, [])
        hits = [ts for ts in hits if now - ts < window_seconds]
        if len(hits) >= max_hits:
            api_rate_limits[bucket_key] = hits
            return True
        hits.append(now)
        api_rate_limits[bucket_key] = hits
    return False


def parse_question_ids(raw):
    if not raw:
        return []
    parsed = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            parsed.append(int(part))
        except ValueError:
            continue
    return parsed


def serialize_question_ids(question_ids):
    return ",".join(str(int(qid)) for qid in question_ids)


def get_exam_setting(conn, key, default="0"):
    cur = conn.cursor()
    cur.execute("SELECT setting_value FROM exam_settings WHERE setting_key = %s", (key,))
    row = cur.fetchone()
    return row[0] if row else default


def set_exam_setting(conn, key, value):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO exam_settings (setting_key, setting_value)
        VALUES (%s, %s)
        ON CONFLICT (setting_key)
        DO UPDATE SET setting_value = EXCLUDED.setting_value
        """,
        (key, str(value)),
    )


def get_active_exam_session(conn):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT *
        FROM exam_sessions
        WHERE is_active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    return cur.fetchone()


def has_exam_started(exam_session):
    if not exam_session:
        return False
    now = datetime.utcnow()
    return now >= exam_session["start_time"]


def is_exam_paused(exam_session):
    return bool(exam_session and exam_session.get("is_paused"))


def get_total_participant_target(conn, exam_session):
    if exam_session and exam_session.get("participant_limit", 0) and exam_session["participant_limit"] > 0:
        return exam_session["participant_limit"]
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users WHERE username <> 'admin'")
    return cur.fetchone()[0]


def ensure_participant(conn, session_id, user_id):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO exam_participants(session_id, user_id, ready_status)
        VALUES (%s, %s, FALSE)
        ON CONFLICT (session_id, user_id) DO NOTHING
        """,
        (session_id, user_id),
    )


def participant_counts(conn, session_id):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN ready_status THEN 1 ELSE 0 END), 0) AS ready_count,
            COALESCE(SUM(CASE WHEN started_at IS NOT NULL AND completed_at IS NULL THEN 1 ELSE 0 END), 0) AS in_progress_count,
            COALESCE(SUM(CASE WHEN completed_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS completed_count
        FROM exam_participants
        WHERE session_id = %s
        """,
        (session_id,),
    )
    row = cur.fetchone()
    return {
        "ready_count": int(row[0] or 0),
        "in_progress_count": int(row[1] or 0),
        "completed_count": int(row[2] or 0),
    }


def resolve_exam_question_ids(conn, exam_session):
    question_ids = parse_question_ids(exam_session.get("question_ids") if exam_session else "")
    if question_ids:
        return question_ids
    cur = conn.cursor()
    cur.execute("SELECT id FROM questions ORDER BY display_order ASC NULLS LAST, id ASC")
    return [row[0] for row in cur.fetchall()]


def get_completed_question_ids(conn, session_id, user_id):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT question_id
        FROM exam_progress
        WHERE session_id = %s AND user_id = %s AND status = 'completed'
        """,
        (session_id, user_id),
    )
    return {row[0] for row in cur.fetchall()}


def current_question_for_user(conn, exam_session, user_id):
    question_ids = resolve_exam_question_ids(conn, exam_session)
    completed = get_completed_question_ids(conn, exam_session["id"], user_id)
    for qid in question_ids:
        if qid not in completed:
            return qid
    return None


def should_use_exam_mode(conn):
    return get_exam_setting(conn, "questions_hidden", "0") == "1"


def get_exam_runtime(conn, user_id=None):
    exam_hidden = should_use_exam_mode(conn)
    active = get_active_exam_session(conn)
    started = has_exam_started(active)
    paused = is_exam_paused(active)

    next_qid = None
    exam_finished = False
    if user_id and active and started and not paused:
        next_qid = current_question_for_user(conn, active, user_id)
        exam_finished = next_qid is None

    return {
        "hidden": exam_hidden,
        "active": active,
        "started": started,
        "paused": paused,
        "next_qid": next_qid,
        "finished": exam_finished,
    }

# ---------- SECURITY ----------
BLOCKED_KEYWORDS = [
    "import os",
    "import sys",
    "import subprocess",
    "import socket",
    "open(",
    "exec(",
    "eval(",
    "__import__",
]

def is_code_safe(code, language):
    """Check if code contains blocked keywords (only for Python)"""
    if language != 'python':
        return True, None
    
    for k in BLOCKED_KEYWORDS:
        if k in code:
            return False, k
    return True, None

def limit_resources():
    """Set resource limits for code execution"""
    resource.setrlimit(resource.RLIMIT_CPU, (5, 5))
    resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))


# ---------- INTERACTIVE C SESSIONS ----------
C_SESSION_TTL_SECONDS = 120
c_sessions = {}
c_sessions_lock = threading.Lock()


def append_c_output(c_session, text):
    with c_session["lock"]:
        c_session["output"] += text
        c_session["updated_at"] = time.time()


def c_session_reader(c_session):
    while True:
        try:
            data = c_session["stdout_pipe"].read(4096)
            if not data:
                break
            append_c_output(c_session, data.decode("utf-8", errors="replace"))
        except Exception:
            break

    exit_code = c_session["proc"].wait()
    with c_session["lock"]:
        c_session["done"] = True
        c_session["exit_code"] = exit_code
        c_session["updated_at"] = time.time()


def prune_c_sessions():
    now = time.time()
    to_delete = []

    with c_sessions_lock:
        for sid, c_session in c_sessions.items():
            with c_session["lock"]:
                expired = c_session["done"] and (now - c_session["updated_at"]) > C_SESSION_TTL_SECONDS
            if expired:
                to_delete.append(sid)

        for sid in to_delete:
            c_session = c_sessions.pop(sid, None)
            if c_session is None:
                continue
            try:
                if c_session["stdin_pipe"]:
                    c_session["stdin_pipe"].close()
            except Exception:
                pass
            try:
                if c_session["stdout_pipe"]:
                    c_session["stdout_pipe"].close()
            except Exception:
                pass
            try:
                c_session["temp_dir"].cleanup()
            except Exception:
                pass


def stop_c_session(session_id):
    with c_sessions_lock:
        c_session = c_sessions.pop(session_id, None)

    if c_session is None:
        return False

    try:
        c_session["proc"].terminate()
    except Exception:
        pass

    try:
        if c_session["stdin_pipe"]:
            c_session["stdin_pipe"].close()
    except Exception:
        pass
    try:
        if c_session["stdout_pipe"]:
            c_session["stdout_pipe"].close()
    except Exception:
        pass
    try:
        c_session["temp_dir"].cleanup()
    except Exception:
        pass

    return True


def compile_c_code(temp_dir, code):
    source_file = os.path.join(temp_dir, "main.c")
    executable = os.path.join(temp_dir, "main")

    with open(source_file, "w", encoding="utf-8") as f:
        f.write(code)

    compile_result = subprocess.run(
        ["gcc", source_file, "-O2", "-std=c11", "-Wall", "-Wextra", "-o", executable],
        capture_output=True,
        text=True,
        timeout=10,
    )

    if compile_result.returncode != 0:
        compile_output = (compile_result.stdout or "") + (compile_result.stderr or "")
        return False, compile_output if compile_output else "Compilation failed", ""

    return True, "", executable


def start_c_session(code):
    temp_dir = tempfile.TemporaryDirectory(prefix="exam_c_")
    try:
        ok, compile_output, executable = compile_c_code(temp_dir.name, code)
        if not ok:
            temp_dir.cleanup()
            return {
                "status": "Compilation failed",
                "session_id": None,
                "output": compile_output,
                "done": True,
                "exit_code": None,
            }

        runner = ["stdbuf", "-o0", "-e0", executable] if os.path.exists("/usr/bin/stdbuf") else [executable]
        proc = subprocess.Popen(
            runner,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=temp_dir.name,
            close_fds=True,
            bufsize=0,
            preexec_fn=limit_resources,
        )

        session_id = uuid.uuid4().hex
        c_session = {
            "session_id": session_id,
            "proc": proc,
            "stdin_pipe": proc.stdin,
            "stdout_pipe": proc.stdout,
            "temp_dir": temp_dir,
            "output": "",
            "done": False,
            "exit_code": None,
            "created_at": time.time(),
            "updated_at": time.time(),
            "lock": threading.Lock(),
        }

        with c_sessions_lock:
            c_sessions[session_id] = c_session

        reader = threading.Thread(target=c_session_reader, args=(c_session,), daemon=True)
        reader.start()

        return {
            "status": "Running",
            "session_id": session_id,
            "output": "",
            "done": False,
            "exit_code": None,
        }
    except Exception:
        temp_dir.cleanup()
        raise

# ---------- CODE EXECUTION ----------
def execute_code(code, language, input_data):
    """Execute code in Python or C"""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            if language == 'python':
                return execute_python(code, input_data, tmpdir)
            elif language == 'c':
                return execute_c(code, input_data, tmpdir)
            else:
                return {
                    'success': False,
                    'output': f'Unsupported language: {language}',
                    'error': 'Unsupported language'
                }
        except Exception as e:
            return {
                'success': False,
                'output': str(e),
                'error': str(e)
            }

def execute_python(code, input_data, tmpdir):
    """Execute Python code"""
    filepath = os.path.join(tmpdir, 'script.py')
    with open(filepath, 'w') as f:
        f.write(code)
    
    try:
        result = subprocess.run(
            ['python3', filepath],
            input=input_data,
            capture_output=True,
            text=True,
            timeout=5,
            preexec_fn=limit_resources
        )
        
        return {
            'success': result.returncode == 0,
            'output': result.stdout,
            'error': result.stderr if result.stderr else None
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'output': 'TIME LIMIT EXCEEDED',
            'error': 'Timeout'
        }
    except Exception as e:
        return {
            'success': False,
            'output': str(e),
            'error': str(e)
        }

def execute_c(code, input_data, tmpdir):
    """Execute C code"""
    source_file = os.path.join(tmpdir, 'main.c')
    executable = os.path.join(tmpdir, 'main')
    
    with open(source_file, 'w') as f:
        f.write(code)
    
    # Compile
    try:
        compile_result = subprocess.run(
            ['gcc', source_file, '-O2', '-std=c11', '-Wall', '-Wextra', '-o', executable],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if compile_result.returncode != 0:
            compile_output = (compile_result.stdout or '') + (compile_result.stderr or '')
            return {
                'success': False,
                'output': compile_output if compile_output else 'Compilation failed',
                'error': None
            }
        
        runner = ['stdbuf', '-o0', '-e0', executable] if os.path.exists('/usr/bin/stdbuf') else [executable]

        # Run
        run_result = subprocess.run(
            runner,
            input=input_data,
            capture_output=True,
            text=True,
            timeout=5,
            preexec_fn=limit_resources
        )
        
        combined_output = (run_result.stdout or '')
        if run_result.stderr:
            combined_output += run_result.stderr
        if run_result.returncode != 0 and not run_result.stderr:
            combined_output += f'Program exited with code {run_result.returncode}\n'

        return {
            'success': run_result.returncode == 0,
            'output': combined_output,
            'error': None
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'output': 'TIME LIMIT EXCEEDED',
            'error': 'Timeout'
        }
    except Exception as e:
        return {
            'success': False,
            'output': str(e),
            'error': str(e)
        }

# ---------- AUTH ----------
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        
        conn = db()
        if not conn:
            return "Database connection error", 500
            
        cur = conn.cursor()
        cur.execute("SELECT id, password FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        close_db(conn)
        
        if not row:
            error = "Invalid username"
        else:
            user_id, hashed = row
            if not hashed or not check_password_hash(hashed, password):
                error = "Invalid password"
            else:
                session["user_id"] = user_id
                session["username"] = username
                if username == "admin":
                    return redirect("/admin/dashboard")

                conn = db()
                if not conn:
                    return "Database connection error", 500
                runtime = get_exam_runtime(conn, user_id=user_id)
                if runtime["hidden"]:
                    if runtime["active"]:
                        ensure_participant(conn, runtime["active"]["id"], user_id)
                        conn.commit()
                    close_db(conn)
                    if runtime["started"] and not runtime["paused"]:
                        if runtime["finished"]:
                            return redirect("/report")
                        return redirect(f"/question/{runtime['next_qid']}")
                    return redirect("/waiting-room")

                close_db(conn)
                return redirect("/")
    
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

# ---------- CORE ----------
@app.route("/")
def questions():
    if "user_id" not in session:
        return redirect("/login")
    preview_mode = request.args.get("preview") == "1"
    if is_admin() and not preview_mode:
        return redirect("/admin/dashboard")
    
    conn = db()
    if not conn:
        return "Database connection error", 500

    runtime = get_exam_runtime(conn, user_id=session["user_id"])
    if runtime["hidden"] and not is_admin():
        if runtime["active"]:
            ensure_participant(conn, runtime["active"]["id"], session["user_id"])
            conn.commit()
        close_db(conn)

        if runtime["started"] and not runtime["paused"]:
            if runtime["finished"]:
                return redirect("/report")
            return redirect(f"/question/{runtime['next_qid']}")
        return redirect("/waiting-room")
        
    cur = conn.cursor()
    cur.execute("""
        SELECT q.id, q.title,
        EXISTS (
            SELECT 1 FROM submissions s
            WHERE s.question_id = q.id
              AND s.user_id = %s
        ) AS completed
        FROM questions q
        ORDER BY q.display_order ASC NULLS LAST, q.id ASC
    """, (session["user_id"],))
    
    questions = cur.fetchall()
    close_db(conn)
    
    return render_template(
        "questions.html",
        questions=questions,
        user=session["username"],
        admin=is_admin()
    )

@app.route("/question/<int:qid>")
def exam_page(qid):
    if not is_logged_in():
        return redirect("/login")
    preview_mode = request.args.get("preview") == "1"
    if is_admin() and not preview_mode:
        return redirect("/admin/dashboard")
    
    conn = db()
    if not conn:
        return "Database connection error", 500

    runtime = get_exam_runtime(conn, user_id=session["user_id"])
    if runtime["hidden"] and not is_admin():
        if not runtime["active"] or not runtime["started"] or runtime["paused"]:
            close_db(conn)
            return redirect("/waiting-room")

        ensure_participant(conn, runtime["active"]["id"], session["user_id"])
        current_qid = current_question_for_user(conn, runtime["active"], session["user_id"])
        if current_qid is None:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE exam_participants
                SET completed_at = COALESCE(completed_at, NOW())
                WHERE session_id = %s AND user_id = %s
                """,
                (runtime["active"]["id"], session["user_id"]),
            )
            conn.commit()
            close_db(conn)
            return redirect("/report")

        if qid != current_qid:
            close_db(conn)
            return redirect(f"/question/{current_qid}")

        cur = conn.cursor()
        cur.execute(
            """
            UPDATE exam_participants
            SET started_at = COALESCE(started_at, NOW())
            WHERE session_id = %s AND user_id = %s
            """,
            (runtime["active"]["id"], session["user_id"]),
        )
        conn.commit()
        
    cur = conn.cursor()
    cur.execute(
        "SELECT title, description, time_limit FROM questions WHERE id = %s",
        (qid,)
    )
    q = cur.fetchone()
    if not q:
        close_db(conn)
        return "Question not found", 404
    
    cur.execute("""
        SELECT input, expected_output
        FROM test_cases
        WHERE question_id = %s AND is_example = 1
        ORDER BY id ASC
        LIMIT 2
    """, (qid,))
    examples = cur.fetchall()
    
    close_db(conn)
    
    # Persist per-question timer deadline in session so refresh does not reset it.
    configured_time_limit = int(q[2]) if q and q[2] else 30 * 60
    now_ts = int(time.time())
    exam_deadlines = session.get("exam_deadlines", {})
    deadline_key = str(qid)

    if deadline_key not in exam_deadlines:
        exam_deadlines[deadline_key] = now_ts + configured_time_limit
        session["exam_deadlines"] = exam_deadlines
        session.modified = True

    remaining_seconds = max(0, int(exam_deadlines[deadline_key]) - now_ts)
    time_limit_minutes = max(1, configured_time_limit // 60)
    
    return render_template(
        "exam.html",
        question_id=qid,
        title=q[0],
        description=q[1],
        examples=examples,
        time_limit=remaining_seconds,
        time_limit_minutes=time_limit_minutes
    )


@app.route("/waiting-room")
def waiting_room():
    if not is_logged_in():
        return redirect("/login")
    if is_admin():
        return redirect("/admin/dashboard")

    conn = db()
    if not conn:
        return "Database connection error", 500

    runtime = get_exam_runtime(conn, user_id=session["user_id"])
    if not runtime["hidden"]:
        close_db(conn)
        return redirect("/")

    if runtime["active"]:
        ensure_participant(conn, runtime["active"]["id"], session["user_id"])
        conn.commit()

    if runtime["started"] and not runtime["paused"] and not runtime["finished"]:
        next_qid = runtime["next_qid"]
        close_db(conn)
        return redirect(f"/question/{next_qid}")

    counts = {"ready_count": 0, "in_progress_count": 0, "completed_count": 0}
    total_target = 0
    start_time = None
    session_name = "Upcoming Exam"
    paused = False
    if runtime["active"]:
        counts = participant_counts(conn, runtime["active"]["id"])
        total_target = get_total_participant_target(conn, runtime["active"])
        start_time = runtime["active"]["start_time"]
        session_name = runtime["active"]["session_name"]
        paused = runtime["paused"]

    close_db(conn)
    return render_template(
        "waiting_room.html",
        session_name=session_name,
        start_time_iso=start_time.isoformat() if start_time else "",
        ready_count=counts["ready_count"],
        total_count=total_target,
        paused=paused,
    )


@app.route("/exam-status")
def exam_status():
    if not is_logged_in():
        return jsonify({"error": "Not logged in"}), 401

    bucket = f"exam-status:{session.get('user_id', 0)}"
    if rate_limited(bucket, max_hits=120, window_seconds=60):
        return jsonify({"error": "Too many requests"}), 429

    conn = db()
    if not conn:
        return jsonify({"error": "Database connection error"}), 500

    runtime = get_exam_runtime(conn, user_id=session["user_id"])
    active = runtime["active"]
    counts = {"ready_count": 0, "in_progress_count": 0, "completed_count": 0}
    total_target = 0
    start_time = None
    end_time = None
    session_name = ""
    if active:
        ensure_participant(conn, active["id"], session["user_id"])
        counts = participant_counts(conn, active["id"])
        total_target = get_total_participant_target(conn, active)
        start_time = active["start_time"]
        end_time = active.get("end_time")
        session_name = active["session_name"]
        conn.commit()

    now = datetime.utcnow()
    seconds_to_start = 0
    if start_time and now < start_time:
        seconds_to_start = int((start_time - now).total_seconds())

    remaining_seconds = None
    if active:
        if start_time and now < start_time:
            remaining_seconds = max(0, int((active.get("duration_minutes", 0) or 0) * 60))
        elif end_time:
            remaining_seconds = max(0, int((end_time - now).total_seconds()))
        else:
            remaining_seconds = max(0, int((active.get("duration_minutes", 0) or 0) * 60))

    redirect_url = None
    if runtime["hidden"] and runtime["started"] and not runtime["paused"] and not runtime["finished"]:
        redirect_url = f"/question/{runtime['next_qid']}"

    close_db(conn)
    return jsonify(
        {
            "hidden": runtime["hidden"],
            "has_session": bool(active),
            "session_name": session_name,
            "started": runtime["started"],
            "paused": runtime["paused"],
            "seconds_to_start": max(0, seconds_to_start),
            "remaining_seconds": remaining_seconds,
            "duration_minutes": int(active.get("duration_minutes", 0)) if active else 0,
            "scheduled_start_iso": start_time.isoformat() if start_time else None,
            "end_time_iso": end_time.isoformat() if end_time else None,
            "ready_count": counts["ready_count"],
            "in_progress_count": counts["in_progress_count"],
            "completed_count": counts["completed_count"],
            "total_count": total_target,
            "redirect_url": redirect_url,
        }
    )


@app.route("/student-ready", methods=["POST"])
def student_ready():
    if not is_logged_in():
        return jsonify({"error": "Not logged in"}), 401

    bucket = f"student-ready:{session.get('user_id', 0)}"
    if rate_limited(bucket, max_hits=20, window_seconds=60):
        return jsonify({"error": "Too many requests"}), 429

    conn = db()
    if not conn:
        return jsonify({"error": "Database connection error"}), 500

    runtime = get_exam_runtime(conn, user_id=session["user_id"])
    if not runtime["hidden"] or not runtime["active"]:
        close_db(conn)
        return jsonify({"error": "No active exam session"}), 400

    ensure_participant(conn, runtime["active"]["id"], session["user_id"])
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE exam_participants
        SET ready_status = TRUE
        WHERE session_id = %s AND user_id = %s
        """,
        (runtime["active"]["id"], session["user_id"]),
    )
    conn.commit()
    close_db(conn)
    return jsonify({"ok": True})

@app.route("/run", methods=["POST"])
def run_code():
    if not is_logged_in():
        return jsonify({"output": "Not logged in", "error": True}), 401
    
    code = request.form.get("code")
    language = request.form.get("language", "python")
    input_data = request.form.get("input", "")
    
    if not code:
        return jsonify({"output": "No code provided", "error": True})
    
    # Security check for Python only
    safe, keyword = is_code_safe(code, language)
    if not safe:
        return jsonify({
            "output": f"Blocked keyword detected: {keyword}",
            "error": True
        })
    
    # Execute code with provided input
    result = execute_code(code, language, input_data)
    
    return jsonify({
        "output": result.get('output', ''),
        "error": result.get('error'),
        "success": result.get('success', False)
    })


@app.route("/run/c/start", methods=["POST"])
def run_c_start():
    if not is_logged_in():
        return jsonify({"error": "Not logged in"}), 401

    prune_c_sessions()
    payload = request.get_json(silent=True) or {}
    code = payload.get("code", "")

    if not isinstance(code, str) or len(code) > 200000:
        return jsonify({"error": "Invalid code payload"}), 400

    try:
        result = start_c_session(code)
        return jsonify(result)
    except subprocess.TimeoutExpired:
        return jsonify({
            "status": "Timed out",
            "session_id": None,
            "output": "Compilation exceeded timeout limits.\n",
            "done": True,
            "exit_code": None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/run/c/input", methods=["POST"])
def run_c_input():
    if not is_logged_in():
        return jsonify({"error": "Not logged in"}), 401

    prune_c_sessions()
    payload = request.get_json(silent=True) or {}
    session_id = payload.get("session_id", "")
    data = payload.get("data", "")

    if not isinstance(session_id, str) or not isinstance(data, str) or len(data) > 4000:
        return jsonify({"error": "Invalid input payload"}), 400

    with c_sessions_lock:
        c_session = c_sessions.get(session_id)

    if c_session is None:
        return jsonify({"error": "Session not found"}), 404

    with c_session["lock"]:
        if c_session["done"]:
            return jsonify({"error": "Session already finished"}), 409

    try:
        c_session["stdin_pipe"].write(data.encode("utf-8", errors="ignore"))
        c_session["stdin_pipe"].flush()
    except Exception:
        return jsonify({"error": "Failed to write input"}), 500

    return jsonify({"ok": True})


@app.route("/run/c/output", methods=["GET"])
def run_c_output():
    if not is_logged_in():
        return jsonify({"error": "Not logged in"}), 401

    prune_c_sessions()
    session_id = request.args.get("session_id", "")
    cursor_raw = request.args.get("cursor", "0")

    try:
        cursor = max(0, int(cursor_raw))
    except ValueError:
        return jsonify({"error": "Invalid cursor"}), 400

    with c_sessions_lock:
        c_session = c_sessions.get(session_id)

    if c_session is None:
        return jsonify({"error": "Session not found"}), 404

    with c_session["lock"]:
        output = c_session["output"][cursor:]
        new_cursor = len(c_session["output"])
        done = c_session["done"]
        exit_code = c_session["exit_code"]
        c_session["updated_at"] = time.time()

    return jsonify({
        "output": output,
        "cursor": new_cursor,
        "done": done,
        "exit_code": exit_code,
    })


@app.route("/run/c/stop", methods=["POST"])
def run_c_stop():
    if not is_logged_in():
        return jsonify({"error": "Not logged in"}), 401

    payload = request.get_json(silent=True) or {}
    session_id = payload.get("session_id", "")
    if not isinstance(session_id, str) or not session_id:
        return jsonify({"error": "Invalid session id"}), 400

    stopped = stop_c_session(session_id)
    if not stopped:
        return jsonify({"error": "Session not found"}), 404

    return jsonify({"ok": True})

@app.route("/submit", methods=["POST"])
def submit():
    if "user_id" not in session:
        return redirect("/login")
    
    user_id = session["user_id"]
    question_id = int(request.form.get("question_id"))
    code = request.form.get("code") or ""
    language = request.form.get("language", "python")
    
    # Security check for Python only
    safe, keyword = is_code_safe(code, language)
    if not safe:
        return render_template(
            "result.html",
            verdict="REJECTED",
            error=f"Blocked keyword detected: {keyword}",
            example_results=[],
            passed=0,
            total=0,
            score=0,
            hidden_passed=0,
            hidden_total=0,
            sequential_mode=False,
            next_url="/",
            auto_seconds=10,
            question_id=question_id
        )
    
    # Load test cases
    conn = db()
    if not conn:
        return "Database connection error", 500

    runtime = get_exam_runtime(conn, user_id=user_id)
    exam_mode = runtime["hidden"] and runtime["active"] and runtime["started"] and not runtime["paused"]
    if exam_mode:
        current_qid = current_question_for_user(conn, runtime["active"], user_id)
        if current_qid != question_id:
            close_db(conn)
            return redirect("/waiting-room")

    cur = conn.cursor()
    cur.execute(
        "SELECT id, input, expected_output FROM test_cases WHERE question_id = %s AND is_example = 1 ORDER BY id",
        (question_id,),
    )
    example_tests = cur.fetchall()

    cur.execute(
        "SELECT id, input, expected_output FROM test_cases WHERE question_id = %s AND is_example = 0 ORDER BY id",
        (question_id,),
    )
    hidden_tests = cur.fetchall()

    example_results = []
    example_test_rows = []
    for i, (tc_id, inp, exp) in enumerate(example_tests, start=1):
        result = execute_code(code, language, inp)
        actual_output = result["output"].strip() if result["success"] else result["output"]
        status = "PASS" if result["success"] and result["output"].strip() == exp.strip() else "FAIL"
        example_results.append(
            {
                "id": i,
                "input": inp,
                "expected": exp.strip(),
                "actual": actual_output,
                "status": status,
            }
        )
        example_test_rows.append(
            {
                "test_case_id": tc_id,
                "case_type": "example",
                "input": inp,
                "expected": exp.strip(),
                "actual": actual_output,
                "status": status,
            }
        )

    hidden_passed = 0
    hidden_total = len(hidden_tests)
    hidden_test_rows = []
    for tc_id, inp, exp in hidden_tests:
        result = execute_code(code, language, inp)
        actual_output = result["output"].strip() if result["success"] else result["output"]
        status = "PASS" if result["success"] and result["output"].strip() == exp.strip() else "FAIL"
        if status == "PASS":
            hidden_passed += 1
        hidden_test_rows.append(
            {
                "test_case_id": tc_id,
                "case_type": "hidden",
                "input": inp,
                "expected": exp.strip(),
                "actual": actual_output,
                "status": status,
            }
        )

    verdict = "PASS" if hidden_passed == hidden_total else "FAIL"
    score = int((hidden_passed / hidden_total) * 100) if hidden_total else 0

    cur.execute(
        """
        INSERT INTO submissions (user_id, question_id, code, verdict, score)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (user_id, question_id, code, verdict, score),
    )
    submission_id = cur.fetchone()[0]

    for row in example_test_rows + hidden_test_rows:
        cur.execute(
            """
            INSERT INTO submission_test_results
                (submission_id, test_case_id, case_type, input_data, expected_output, actual_output, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                submission_id,
                row["test_case_id"],
                row["case_type"],
                row["input"],
                row["expected"],
                row["actual"],
                row["status"],
            ),
        )

    next_url = "/"
    sequential_mode = bool(exam_mode)
    has_next_question = False
    if exam_mode:
        ensure_participant(conn, runtime["active"]["id"], user_id)
        cur.execute(
            """
            INSERT INTO exam_progress(session_id, user_id, question_id, status, verdict, score, time_spent)
            VALUES (%s, %s, %s, 'completed', %s, %s, 0)
            ON CONFLICT (session_id, user_id, question_id)
            DO UPDATE SET status = 'completed', verdict = EXCLUDED.verdict, score = EXCLUDED.score
            """,
            (runtime["active"]["id"], user_id, question_id, verdict, score),
        )

        next_qid = current_question_for_user(conn, runtime["active"], user_id)
        if next_qid is None:
            cur.execute(
                """
                UPDATE exam_participants
                SET completed_at = NOW()
                WHERE session_id = %s AND user_id = %s
                """,
                (runtime["active"]["id"], user_id),
            )
            next_url = "/report"
        else:
            has_next_question = True
            next_url = f"/question/{next_qid}"

    conn.commit()
    close_db(conn)
    
    return render_template(
        "result.html",
        verdict=verdict,
        total=hidden_total,
        passed=hidden_passed,
        score=score,
        hidden_total=hidden_total,
        hidden_passed=hidden_passed,
        example_results=example_results,
        sequential_mode=sequential_mode,
        has_next_question=has_next_question,
        next_url=next_url,
        auto_seconds=10,
        question_id=question_id
    )

# ---------- ADMIN ROUTES ----------
# ---------- ADMIN ROUTES ----------
@app.route("/admin/dashboard")
def admin_dashboard():
    if not is_logged_in() or not is_admin():
        return redirect("/")
    
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    
    # Get statistics
    cur.execute("SELECT COUNT(*) FROM questions")
    total_questions = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM submissions")
    total_submissions = cur.fetchone()[0]
    
    # Get recent questions with time_limit
    cur.execute("""
        SELECT q.id, q.title, COUNT(t.id) as test_count, q.time_limit
        FROM questions q
        LEFT JOIN test_cases t ON t.question_id = q.id
        GROUP BY q.id
        ORDER BY q.display_order ASC NULLS LAST, q.id ASC
        LIMIT 5
    """)
    recent_questions = cur.fetchall()
    
    # Calculate success rate
    cur.execute("SELECT COUNT(*) FROM submissions WHERE verdict = 'PASS'")
    passed = cur.fetchone()[0]
    success_rate = int((passed / total_submissions * 100)) if total_submissions > 0 else 0

    close_db(conn)
    
    stats = {
        'total_questions': total_questions,
        'total_users': total_users,
        'total_submissions': total_submissions,
        'questions_this_month': min(5, total_questions),
        'new_users': min(3, total_users),
        'submissions_today': min(10, total_submissions),
        'success_rate': success_rate
    }
    
    return render_template(
        "admin_dashboard.html",
        stats=stats,
        recent_questions=recent_questions
    )


@app.route("/admin/schedule-exam", methods=["GET"])
def admin_schedule_exam():
    if not is_logged_in() or not is_admin():
        return redirect("/")
    return redirect("/admin/exam-center#exam-scheduling")


@app.route("/admin/exam-center")
def admin_exam_center():
    if not is_logged_in() or not is_admin():
        return redirect("/")

    conn = db()
    if not conn:
        return "Database connection error", 500

    runtime = get_exam_runtime(conn)
    cur = conn.cursor()
    cur.execute("SELECT id, title FROM questions ORDER BY display_order ASC NULLS LAST, id ASC")
    all_questions = cur.fetchall()

    cur_dict = conn.cursor(cursor_factory=RealDictCursor)
    cur_dict.execute(
        """
        SELECT id, session_name, start_time, end_time, duration_minutes, is_active, question_ids, created_at
        FROM exam_sessions
        ORDER BY created_at DESC
        LIMIT 50
        """
    )
    existing_sessions = cur_dict.fetchall()
    monitor = {"ready_count": 0, "in_progress_count": 0, "completed_count": 0, "total_count": 0}
    if runtime["active"]:
        monitor.update(participant_counts(conn, runtime["active"]["id"]))
        monitor["total_count"] = get_total_participant_target(conn, runtime["active"])

    close_db(conn)
    return render_template(
        "admin_exam_center.html",
        exam_hidden=runtime["hidden"],
        active_exam=runtime["active"],
        exam_started=runtime["started"],
        exam_paused=runtime["paused"],
        monitor=monitor,
        all_questions=all_questions,
        existing_sessions=existing_sessions,
    )


@app.route("/admin/create-session", methods=["POST"])
def admin_create_session():
    if not is_logged_in() or not is_admin():
        return redirect("/")

    session_name = (request.form.get("session_name") or "Scheduled Exam").strip()
    start_time_raw = request.form.get("start_time")
    duration_minutes = int(request.form.get("duration_minutes") or "60")
    session_id_raw = request.form.get("session_id")
    question_ids = [int(qid) for qid in request.form.getlist("question_ids") if str(qid).isdigit()]

    if not start_time_raw:
        return redirect("/admin/exam-center")

    try:
        normalized = start_time_raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            start_time = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            start_time = parsed
    except ValueError:
        return redirect("/admin/exam-center")

    end_time = start_time + timedelta(minutes=duration_minutes)
    selected_qids = serialize_question_ids(question_ids)

    conn = db()
    if not conn:
        return "Database connection error", 500

    cur = conn.cursor()
    if session_id_raw and str(session_id_raw).isdigit():
        session_id = int(session_id_raw)
        cur.execute(
            """
            UPDATE exam_sessions
            SET session_name = %s,
                start_time = %s,
                end_time = %s,
                duration_minutes = %s,
                question_ids = %s
            WHERE id = %s
            """,
            (session_name, start_time, end_time, duration_minutes, selected_qids, session_id),
        )
    else:
        cur.execute("UPDATE exam_sessions SET is_active = FALSE WHERE is_active = TRUE")
        cur.execute(
            """
            INSERT INTO exam_sessions(
                session_name, start_time, end_time, duration_minutes, is_active, question_ids,
                participant_limit, auto_start_when_all_ready, is_paused
            ) VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s, FALSE)
            """,
            (
                session_name,
                start_time,
                end_time,
                duration_minutes,
                selected_qids,
                0,
                False,
            ),
        )
    conn.commit()
    close_db(conn)
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": True})
    return redirect("/admin/exam-center#exam-scheduling")


@app.route("/admin/toggle-visibility", methods=["POST"])
def admin_toggle_visibility():
    if not is_logged_in() or not is_admin():
        return redirect("/")

    target = request.form.get("hidden", "0")
    conn = db()
    if not conn:
        return "Database connection error", 500

    set_exam_setting(conn, "questions_hidden", "1" if target == "1" else "0")
    conn.commit()
    close_db(conn)
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": True, "hidden": target == "1"})
    return redirect("/admin/exam-center#exam-controls")


@app.route("/admin/delete-session/<int:session_id>", methods=["POST"])
def admin_delete_session(session_id):
    if not is_logged_in() or not is_admin():
        return redirect("/")

    conn = db()
    if not conn:
        return "Database connection error", 500

    cur = conn.cursor()
    cur.execute("DELETE FROM exam_sessions WHERE id = %s", (session_id,))
    conn.commit()
    close_db(conn)

    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": True, "deleted_id": session_id})
    return redirect("/admin/exam-center#exam-scheduling")


@app.route("/admin/exam-controls", methods=["POST"])
def admin_exam_controls():
    if not is_logged_in() or not is_admin():
        return redirect("/")

    action = request.form.get("action", "")
    minutes = int(request.form.get("minutes") or "0")

    conn = db()
    if not conn:
        return "Database connection error", 500

    active = get_active_exam_session(conn)
    if active:
        cur = conn.cursor()
        if action == "force_start":
            start_now = datetime.utcnow()
            end_at = start_now + timedelta(minutes=int(active.get("duration_minutes", 0) or 0))
            cur.execute(
                """
                UPDATE exam_sessions
                SET start_time = %s,
                    end_time = %s,
                    is_paused = FALSE,
                    paused_at = NULL
                WHERE id = %s
                """,
                (start_now, end_at, active["id"]),
            )
        elif action == "stop_session":
            cur.execute(
                """
                UPDATE exam_sessions
                SET is_active = FALSE,
                    is_paused = FALSE,
                    paused_at = NULL,
                    end_time = COALESCE(end_time, NOW())
                WHERE id = %s
                """,
                (active["id"],),
            )
        elif action == "pause":
            cur.execute("UPDATE exam_sessions SET is_paused = TRUE, paused_at = NOW() WHERE id = %s", (active["id"],))
        elif action == "resume":
            cur.execute(
                """
                UPDATE exam_sessions
                SET is_paused = FALSE,
                    start_time = CASE
                        WHEN paused_at IS NOT NULL THEN start_time + (NOW() - paused_at)
                        ELSE start_time
                    END,
                    end_time = CASE
                        WHEN paused_at IS NOT NULL AND end_time IS NOT NULL THEN end_time + (NOW() - paused_at)
                        ELSE end_time
                    END,
                    paused_at = NULL
                WHERE id = %s
                """,
                (active["id"],),
            )
        elif action == "add_time" and minutes > 0:
            cur.execute(
                """
                UPDATE exam_sessions
                SET end_time = CASE
                    WHEN end_time IS NULL THEN NOW() + (%s || ' minutes')::interval
                    ELSE end_time + (%s || ' minutes')::interval
                END,
                duration_minutes = duration_minutes + %s
                WHERE id = %s
                """,
                (minutes, minutes, minutes, active["id"]),
            )

    conn.commit()
    close_db(conn)
    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": True, "action": action})
    return redirect("/admin/exam-center#live-monitor")


@app.route("/admin/clear-exam-submissions", methods=["POST"])
def admin_clear_exam_submissions():
    if not is_logged_in() or not is_admin():
        if request.headers.get("X-Requested-With") == "fetch":
            return jsonify({"ok": False, "error": "Unauthorized"}), 403
        return redirect("/")

    conn = db()
    if not conn:
        if request.headers.get("X-Requested-With") == "fetch":
            return jsonify({"ok": False, "error": "Database connection error"}), 500
        return "Database connection error", 500

    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM submissions s
        WHERE EXISTS (
            SELECT 1
            FROM exam_sessions es
            WHERE s.timestamp >= es.start_time
              AND s.timestamp <= COALESCE(
                  es.end_time,
                  es.start_time + (es.duration_minutes || ' minutes')::interval
              )
              AND es.question_ids IS NOT NULL
              AND es.question_ids <> ''
              AND (',' || REPLACE(es.question_ids, ' ', '') || ',') LIKE ('%%,' || s.question_id::text || ',%%')
        )
        """
    )
    deleted_count = cur.rowcount
    conn.commit()
    close_db(conn)

    if request.headers.get("X-Requested-With") == "fetch":
        return jsonify({"ok": True, "deleted_count": deleted_count})
    return redirect("/admin/exam-center#live-monitor")

@app.route("/admin/questions")
def admin_questions():
    if not is_logged_in() or not is_admin():
        return redirect("/")

    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()

    cur.execute("""
        SELECT q.id, q.title, COUNT(t.id), q.display_order
        FROM questions q
        LEFT JOIN test_cases t ON t.question_id = q.id
        GROUP BY q.id
        ORDER BY q.display_order ASC NULLS LAST, q.id ASC
    """)
    questions = cur.fetchall()
    close_db(conn)

    return render_template("admin_questions.html", questions=questions)


@app.route("/admin/questions/reorder", methods=["POST"])
def admin_reorder_questions():
    if not is_logged_in() or not is_admin():
        return redirect("/")

    qid = request.form.get("qid", type=int)
    direction = (request.form.get("direction") or "").strip().lower()
    if not qid or direction not in {"up", "down"}:
        return redirect("/admin/questions")

    conn = db()
    if not conn:
        return "Database connection error", 500

    cur = conn.cursor()
    cur.execute("SELECT id FROM questions ORDER BY display_order ASC NULLS LAST, id ASC")
    ordered_ids = [row[0] for row in cur.fetchall()]

    if qid in ordered_ids:
        idx = ordered_ids.index(qid)
        swap_idx = idx - 1 if direction == "up" else idx + 1
        if 0 <= swap_idx < len(ordered_ids):
            ordered_ids[idx], ordered_ids[swap_idx] = ordered_ids[swap_idx], ordered_ids[idx]
            for pos, question_id in enumerate(ordered_ids, start=1):
                cur.execute(
                    "UPDATE questions SET display_order = %s WHERE id = %s",
                    (pos, question_id),
                )
            conn.commit()

    close_db(conn)
    return redirect("/admin/questions")


@app.route("/admin/questions/reorder-all", methods=["POST"])
def admin_reorder_questions_all():
    if not is_logged_in() or not is_admin():
        return jsonify({"ok": False, "error": "Unauthorized"}), 403

    payload = request.get_json(silent=True) or {}
    qids = payload.get("question_ids")
    if not isinstance(qids, list):
        return jsonify({"ok": False, "error": "Invalid payload"}), 400

    normalized = []
    seen = set()
    for raw in qids:
        try:
            qid = int(raw)
        except (TypeError, ValueError):
            continue
        if qid in seen:
            continue
        normalized.append(qid)
        seen.add(qid)

    conn = db()
    if not conn:
        return jsonify({"ok": False, "error": "Database connection error"}), 500

    cur = conn.cursor()
    cur.execute("SELECT id FROM questions ORDER BY display_order ASC NULLS LAST, id ASC")
    existing_ids = [row[0] for row in cur.fetchall()]
    existing_set = set(existing_ids)

    if set(normalized) != existing_set:
        close_db(conn)
        return jsonify({"ok": False, "error": "Question list mismatch"}), 400

    for pos, question_id in enumerate(normalized, start=1):
        cur.execute(
            "UPDATE questions SET display_order = %s WHERE id = %s",
            (pos, question_id),
        )

    conn.commit()
    close_db(conn)
    return jsonify({"ok": True})

@app.route("/admin/delete/<int:qid>", methods=["POST"])
def delete_question(qid):
    if not is_logged_in() or not is_admin():
        return redirect("/")

    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()

    # Delete test cases first
    cur.execute("DELETE FROM test_cases WHERE question_id = %s", (qid,))
    # Delete submissions related to this question
    cur.execute("DELETE FROM submissions WHERE question_id = %s", (qid,))
    # Delete the question
    cur.execute("DELETE FROM questions WHERE id = %s", (qid,))

    # Keep ordering contiguous after deletion
    cur.execute("SELECT id FROM questions ORDER BY display_order ASC NULLS LAST, id ASC")
    ordered_ids = [row[0] for row in cur.fetchall()]
    for pos, question_id in enumerate(ordered_ids, start=1):
        cur.execute("UPDATE questions SET display_order = %s WHERE id = %s", (pos, question_id))

    conn.commit()
    close_db(conn)

    return redirect("/admin/questions")

@app.route("/admin", methods=["GET", "POST"])
def admin():
    if not is_logged_in() or not is_admin():
        return redirect("/")

    if request.method == "POST":
        title = request.form.get("title")
        description = request.form.get("description")
        marks = request.form.get("marks", 10)
        time_limit = int(request.form.get("time_limit", 30)) * 60  # Convert minutes to seconds
        inputs = request.form.getlist("input[]")
        outputs = request.form.getlist("output[]")
        examples = request.form.getlist("is_example[]")

        conn = db()
        if not conn:
            return "Database connection error", 500
            
        cur = conn.cursor()

        # Insert question with time_limit
        cur.execute("SELECT COALESCE(MAX(display_order), 0) FROM questions")
        next_display_order = int(cur.fetchone()[0] or 0) + 1
        cur.execute(
            "INSERT INTO questions (title, description, marks, time_limit, display_order) VALUES (%s, %s, %s, %s, %s)",
            (title, description, marks, time_limit, next_display_order)
        )
        
        # Get the ID of the inserted question
        cur.execute("SELECT lastval()")
        qid = cur.fetchone()[0]

        # Insert test cases
        for i in range(len(inputs)):
            is_example = 1 if str(i) in examples else 0
            
            cur.execute(
                """
                INSERT INTO test_cases (question_id, input, expected_output, is_example)
                VALUES (%s, %s, %s, %s)
                """,
                (qid, inputs[i], outputs[i], is_example)
            )

        conn.commit()
        close_db(conn)

        return redirect("/admin/dashboard")

    return render_template("admin.html")

@app.route("/admin/edit/<int:qid>", methods=["GET", "POST"])
def edit_question(qid):
    if not is_logged_in() or not is_admin():
        return redirect("/")

    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()

    if request.method == "POST":
        title = request.form.get("title")
        description = request.form.get("description")
        time_limit = int(request.form.get("time_limit", 30)) * 60
        inputs = request.form.getlist("input[]")
        outputs = request.form.getlist("output[]")
        examples = request.form.getlist("is_example[]")

        # Update question
        cur.execute(
            "UPDATE questions SET title = %s, description = %s, time_limit = %s WHERE id = %s",
            (title, description, time_limit, qid)
        )

        # Remove old test cases
        cur.execute("DELETE FROM test_cases WHERE question_id = %s", (qid,))

        # Insert new test cases
        for i in range(len(inputs)):
            is_example = 1 if str(i) in examples else 0
            
            cur.execute(
                """
                INSERT INTO test_cases (question_id, input, expected_output, is_example)
                VALUES (%s, %s, %s, %s)
                """,
                (qid, inputs[i], outputs[i], is_example)
            )

        conn.commit()
        close_db(conn)

        return redirect("/admin/questions")

    # GET: load existing data
    cur.execute(
        "SELECT title, description, time_limit FROM questions WHERE id = %s",
        (qid,)
    )
    question = cur.fetchone()

    cur.execute(
        """SELECT input, expected_output, is_example
        FROM test_cases
        WHERE question_id = %s
        ORDER BY id
        """,
        (qid,)
    )
    test_cases = cur.fetchall()

    close_db(conn)

    return render_template(
        "admin_edit.html",
        question=question,
        test_cases=test_cases,
        qid=qid
    )

@app.route("/admin/users")
def admin_users():
    if not is_logged_in() or not is_admin():
        return redirect("/")
    
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    cur.execute("SELECT id, username FROM users ORDER BY id")
    users = cur.fetchall()
    close_db(conn)
    
    return render_template("admin_users.html", users=users)

@app.route("/admin/users/<int:user_id>/delete", methods=["POST"])
def delete_user(user_id):
    if not is_logged_in() or not is_admin():
        return jsonify({"ok": False, "error": "Unauthorized"}), 403

    conn = db()
    if not conn:
        return jsonify({"ok": False, "error": "Database connection error"}), 500

    try:
        cur = conn.cursor()
        cur.execute("SELECT id, username FROM users WHERE id = %s", (user_id,))
        user = cur.fetchone()

        if not user:
            return jsonify({"ok": False, "error": "User not found"}), 404

        username = user[1]
        if username == "admin":
            return jsonify({"ok": False, "error": "Cannot delete admin user"}), 400

        if session.get("user_id") == user_id:
            return jsonify({"ok": False, "error": "Cannot delete your own account"}), 400

        cur.execute("DELETE FROM exam_participants WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM exam_progress WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM submissions WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()

        return jsonify({"ok": True, "user_id": user_id, "username": username})
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "Failed to delete user"}), 500
    finally:
        close_db(conn)

# ---------- ADMIN USER REPORTS ----------

@app.route("/admin/user_reports")
def admin_user_reports():
    if not is_logged_in() or not is_admin():
        return redirect("/")
    
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    
    # Get all users with their submission stats
    cur.execute("""
        SELECT 
            u.id, 
            u.username, 
            COALESCE(COUNT(s.id), 0) as total_submissions,
            COALESCE(SUM(CASE WHEN s.verdict = 'PASS' THEN 1 ELSE 0 END), 0) as passed,
            COALESCE(AVG(s.score), 0) as avg_score
        FROM users u
        LEFT JOIN submissions s ON s.user_id = u.id
        GROUP BY u.id, u.username
        ORDER BY u.username
    """)
    users_data = cur.fetchall()
    
    # Format the data
    user_stats = []
    for user in users_data:
        user_id, username, total, passed, avg_score = user
        avg_score = round(avg_score, 1)
        success_rate = int((passed / total * 100)) if total > 0 else 0
        
        user_stats.append({
            'id': user_id,
            'username': username,
            'total_submissions': total,
            'passed': passed,
            'avg_score': avg_score,
            'success_rate': success_rate
        })
    
    close_db(conn)
    
    return render_template("admin_user_reports.html", users=user_stats)

@app.route("/admin/user_reports/<int:user_id>")
def admin_user_detail(user_id):
    if not is_logged_in() or not is_admin():
        return redirect("/")
    
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    
    # Get user info
    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
    user = cur.fetchone()
    
    if not user:
        close_db(conn)
        return "User not found", 404
    
    username = user[0]

    # Match the display ID used in /admin/user_reports (ordered by username, then id)
    cur.execute(
        """
        SELECT display_id
        FROM (
            SELECT id, ROW_NUMBER() OVER (ORDER BY username, id) AS display_id
            FROM users
        ) ranked
        WHERE id = %s
        """,
        (user_id,),
    )
    display_row = cur.fetchone()
    display_user_id = display_row[0] if display_row else user_id
    
    # Get user's submissions with question details and classify exam/practice mode.
    # Exam classification is inferred from whether the submission time falls within
    # an exam session window and the question belongs to that session's question set.
    cur.execute("""
        SELECT
            s.id,
            q.title,
            s.verdict,
            s.score,
            s.timestamp,
            s.question_id,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM exam_sessions es
                    WHERE s.timestamp >= es.start_time
                      AND s.timestamp <= COALESCE(
                          es.end_time,
                          es.start_time + (es.duration_minutes || ' minutes')::interval
                      )
                      AND es.question_ids IS NOT NULL
                      AND es.question_ids <> ''
                      AND (',' || REPLACE(es.question_ids, ' ', '') || ',') LIKE ('%%,' || s.question_id::text || ',%%')
                ) THEN 'exam'
                ELSE 'practice'
            END AS submission_mode
        FROM submissions s
        JOIN questions q ON s.question_id = q.id
        WHERE s.user_id = %s
        ORDER BY s.timestamp DESC
    """, (user_id,))
    
    submissions = cur.fetchall()
    exam_submissions = [s for s in submissions if s[6] == "exam"]
    practice_submissions = [s for s in submissions if s[6] == "practice"]
    
    # Calculate statistics
    total_submissions = len(submissions)
    passed = sum(1 for s in submissions if s[2] == 'PASS')
    success_rate = int((passed / total_submissions * 100)) if total_submissions > 0 else 0
    avg_score = round(sum(s[3] for s in submissions) / total_submissions, 1) if total_submissions > 0 else 0
    
    # Get all questions to show completion status
    cur.execute("SELECT id, title FROM questions ORDER BY display_order ASC NULLS LAST, id ASC")
    all_questions = cur.fetchall()
    
    # Get which questions user has attempted
    cur.execute("SELECT DISTINCT question_id FROM submissions WHERE user_id = %s", (user_id,))
    attempted_questions = [row[0] for row in cur.fetchall()]
    
    question_status = []
    for qid, title in all_questions:
        status = "Attempted" if qid in attempted_questions else "Not Attempted"
        question_status.append({
            'id': qid,
            'title': title,
            'status': status
        })
    
    close_db(conn)
    
    return render_template(
        "admin_user_detail.html",
        user_id=user_id,
        display_user_id=display_user_id,
        username=username,
        submissions=submissions,
        exam_submissions=exam_submissions,
        practice_submissions=practice_submissions,
        total_submissions=total_submissions,
        passed=passed,
        success_rate=success_rate,
        avg_score=avg_score,
        question_status=question_status
    )

@app.route("/admin/submission/<int:submission_id>")
def admin_submission_detail(submission_id):
    if not is_logged_in() or not is_admin():
        return redirect("/")

    conn = db()
    if not conn:
        return "Database connection error", 500

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT s.id,
               s.user_id,
               u.username,
               q.title AS question_title,
               s.question_id,
               s.verdict,
               s.score,
               s.timestamp,
               s.code
        FROM submissions s
        JOIN users u ON s.user_id = u.id
        JOIN questions q ON s.question_id = q.id
        WHERE s.id = %s
        """,
        (submission_id,),
    )
    submission = cur.fetchone()
    if not submission:
        close_db(conn)
        return "Submission not found", 404

    cur.execute(
        """
        SELECT case_type,
               input_data,
               expected_output,
               actual_output,
               status
        FROM submission_test_results
        WHERE submission_id = %s
        ORDER BY
            CASE WHEN case_type = 'example' THEN 0 ELSE 1 END,
            id ASC
        """,
        (submission_id,),
    )
    test_results = cur.fetchall()
    close_db(conn)

    return render_template(
        "admin_submission_detail.html",
        submission=submission,
        test_results=test_results,
    )

@app.route("/admin/reports")
def admin_reports():
    if not is_logged_in() or not is_admin():
        return redirect("/")
    
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    
    # Get submission statistics
    cur.execute("""
        SELECT 
            u.username, 
            q.title, 
            COALESCE(s.verdict, 'NO SUBMISSION') as verdict,
            COALESCE(s.score, 0) as score,
            COALESCE(s.timestamp, 'N/A') as timestamp
        FROM users u
        CROSS JOIN questions q
        LEFT JOIN submissions s ON s.user_id = u.id AND s.question_id = q.id
        ORDER BY s.timestamp DESC, u.username, q.title
        LIMIT 100
    """)
    submissions = cur.fetchall()
    
    # Get unique users and questions for filters
    cur.execute("SELECT DISTINCT username FROM users ORDER BY username")
    unique_users = [row[0] for row in cur.fetchall()]
    
    cur.execute("SELECT DISTINCT title FROM questions ORDER BY title")
    unique_questions = [row[0] for row in cur.fetchall()]
    
    # Calculate statistics
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]
    
    # Calculate success rate (only for actual submissions)
    cur.execute("SELECT COUNT(*) FROM submissions WHERE verdict = 'PASS'")
    passed = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM submissions")
    total_subs = cur.fetchone()[0]
    success_rate = int((passed / total_subs * 100)) if total_subs > 0 else 0
    
    # Calculate average score
    cur.execute("SELECT AVG(score) FROM submissions WHERE score IS NOT NULL")
    avg_score_result = cur.fetchone()[0]
    avg_score = round(avg_score_result, 1) if avg_score_result else 0
    
    close_db(conn)
    
    return render_template(
        "admin_reports.html",
        submissions=submissions,
        unique_users=unique_users,
        unique_questions=unique_questions,
        total_users=total_users,
        success_rate=success_rate,
        avg_score=avg_score
    )

@app.route("/report")
def report():
    if not is_logged_in():
        return redirect("/login")
    if is_admin():
        return redirect("/admin/dashboard")
    
    conn = db()
    if not conn:
        return "Database connection error", 500

    runtime = get_exam_runtime(conn, user_id=session["user_id"])
    back_url = "/waiting-room" if runtime["hidden"] else "/"
    selected_questions = []
    report_session = runtime["active"]

    if not report_session:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT es.*
            FROM exam_sessions es
            WHERE es.id = (
                SELECT ep.session_id
                FROM exam_progress ep
                WHERE ep.user_id = %s AND ep.session_id IS NOT NULL
                ORDER BY ep.id DESC
                LIMIT 1
            )
            """,
            (session["user_id"],),
        )
        report_session = cur.fetchone()

    if report_session:
        selected_questions = resolve_exam_question_ids(conn, report_session)

    cur = conn.cursor()

    if report_session and selected_questions:
        cur.execute(
            """
            SELECT q.title,
                   CASE
                       WHEN ep.status = 'completed' THEN COALESCE(
                           ep.verdict,
                           CASE WHEN COALESCE(ep.score, 0) = 100 THEN 'PASS' ELSE 'FAIL' END
                       )
                       WHEN ep.status IS NULL THEN 'NO ATTEMPT'
                       ELSE UPPER(ep.status)
                   END as verdict,
                   COALESCE(ep.score, 0) as score
            FROM questions q
            LEFT JOIN exam_progress ep
                   ON ep.question_id = q.id
                  AND ep.user_id = %s
                  AND ep.session_id = %s
            WHERE q.id = ANY(%s)
            ORDER BY array_position(%s::int[], q.id)
            """,
            (session["user_id"], report_session["id"], selected_questions, selected_questions),
        )
    else:
        rows = []
        total = 0
        close_db(conn)
        return render_template("report.html", rows=rows, total=total, back_url=back_url)
    
    rows = cur.fetchall()
    total = sum(r[2] for r in rows if r[2] and r[1] != 'NO ATTEMPT') if rows else 0
    
    close_db(conn)
    
    return render_template("report.html", rows=rows, total=total, back_url=back_url)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
