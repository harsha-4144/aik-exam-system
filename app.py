import os
from flask import Flask, render_template, request, redirect, session, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import resource
import tempfile
import threading
import time
import uuid
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "exam-secret-key-change-this")

# ---------- DATABASE ----------
def get_db():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def db():
    return get_db()

def is_logged_in():
    return "user_id" in session

def is_admin():
    return session.get("username") == "admin"

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
        conn.close()
        
        if not row:
            error = "Invalid username"
        else:
            user_id, hashed = row
            if not hashed or not check_password_hash(hashed, password):
                error = "Invalid password"
            else:
                session["user_id"] = user_id
                session["username"] = username
                return redirect("/")
    
    return render_template("login.html", error=error)

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        
        if not username or not password:
            return "Username and password required"
        
        hashed = generate_password_hash(password)
        
        conn = db()
        if not conn:
            return "Database connection error", 500
            
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        if cur.fetchone():
            conn.close()
            return "User already exists. Please login."
        
        cur.execute(
            "INSERT INTO users (username, password) VALUES (%s, %s)",
            (username, hashed)
        )
        conn.commit()
        conn.close()
        
        return redirect("/login")
    
    return render_template("register.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

# ---------- CORE ----------
@app.route("/")
def questions():
    if "user_id" not in session:
        return redirect("/login")
    
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    cur.execute("""
        SELECT q.id, q.title,
        EXISTS (
            SELECT 1 FROM submissions s
            WHERE s.question_id = q.id
              AND s.user_id = %s
        ) AS completed
        FROM questions q
        ORDER BY q.id
    """, (session["user_id"],))
    
    questions = cur.fetchall()
    conn.close()
    
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
    
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    cur.execute(
        "SELECT title, description, time_limit FROM questions WHERE id = %s",
        (qid,)
    )
    q = cur.fetchone()
    
    cur.execute("""
        SELECT input, expected_output
        FROM test_cases
        WHERE question_id = %s AND is_example = 1
        ORDER BY id ASC
        LIMIT 2
    """, (qid,))
    examples = cur.fetchall()
    
    conn.close()
    
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
            results=[],
            passed=0,
            total=0,
            score=0,
            question_id=question_id
        )
    
    # Load test cases (only non-example ones for grading)
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    cur.execute(
        "SELECT input, expected_output FROM test_cases WHERE question_id = %s AND is_example = 0",
        (question_id,)
    )
    tests = cur.fetchall()
    conn.close()
    
    results = []
    passed_count = 0
    total_tests = len(tests)
    
    # Execute against all test cases
    for i, (inp, exp) in enumerate(tests, start=1):
        result = execute_code(code, language, inp)
        
        if result['success'] and result['output'].strip() == exp.strip():
            status = "PASS"
            passed_count += 1
        else:
            status = "FAIL"
        
        results.append({
            "id": i,
            "input": inp,
            "expected": exp.strip(),
            "actual": result['output'].strip() if result['success'] else result['output'],
            "status": status,
            "error": result['error']
        })
    
    # Final verdict
    verdict = "PASS" if passed_count == total_tests else "FAIL"
    score = int((passed_count / total_tests) * 100) if total_tests else 0
    
    # Save submission
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO submissions (user_id, question_id, code, verdict, score) VALUES (%s, %s, %s, %s, %s)",
        (user_id, question_id, code, verdict, score)
    )
    conn.commit()
    conn.close()
    
    return render_template(
        "result.html",
        verdict=verdict,
        total=total_tests,
        passed=passed_count,
        score=score,
        results=results,
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
        ORDER BY q.id DESC
        LIMIT 5
    """)
    recent_questions = cur.fetchall()
    
    # Calculate success rate
    cur.execute("SELECT COUNT(*) FROM submissions WHERE verdict = 'PASS'")
    passed = cur.fetchone()[0]
    success_rate = int((passed / total_submissions * 100)) if total_submissions > 0 else 0
    
    conn.close()
    
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

@app.route("/admin/questions")
def admin_questions():
    if not is_logged_in() or not is_admin():
        return redirect("/")

    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()

    cur.execute("""
        SELECT q.id, q.title, COUNT(t.id)
        FROM questions q
        LEFT JOIN test_cases t ON t.question_id = q.id
        GROUP BY q.id
        ORDER BY q.id
    """)
    questions = cur.fetchall()
    conn.close()

    return render_template("admin_questions.html", questions=questions)

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

    conn.commit()
    conn.close()

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
        cur.execute(
            "INSERT INTO questions (title, description, marks, time_limit) VALUES (%s, %s, %s, %s)",
            (title, description, marks, time_limit)
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
        conn.close()

        return redirect("/admin/questions")

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
        conn.close()

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

    conn.close()

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
    conn.close()
    
    return render_template("admin_users.html", users=users)

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
    
    conn.close()
    
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
        conn.close()
        return "User not found", 404
    
    username = user[0]
    
    # Get user's submissions with question details
    cur.execute("""
        SELECT 
            q.title, 
            s.verdict, 
            s.score,
            s.timestamp,
            s.question_id
        FROM submissions s
        JOIN questions q ON s.question_id = q.id
        WHERE s.user_id = %s
        ORDER BY s.timestamp DESC
    """, (user_id,))
    
    submissions = cur.fetchall()
    
    # Calculate statistics
    total_submissions = len(submissions)
    passed = sum(1 for s in submissions if s[1] == 'PASS')
    success_rate = int((passed / total_submissions * 100)) if total_submissions > 0 else 0
    avg_score = round(sum(s[2] for s in submissions) / total_submissions, 1) if total_submissions > 0 else 0
    
    # Get all questions to show completion status
    cur.execute("SELECT id, title FROM questions ORDER BY id")
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
    
    conn.close()
    
    return render_template(
        "admin_user_detail.html",
        user_id=user_id,
        username=username,
        submissions=submissions,
        total_submissions=total_submissions,
        passed=passed,
        success_rate=success_rate,
        avg_score=avg_score,
        question_status=question_status
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
    
    conn.close()
    
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
    
    conn = db()
    if not conn:
        return "Database connection error", 500
        
    cur = conn.cursor()
    
    # Get user's submission summary
    cur.execute("""
        SELECT q.title, 
               CASE 
                   WHEN s.verdict IS NULL THEN 'NO ATTEMPT'
                   ELSE s.verdict 
               END as verdict,
               COALESCE(s.score, 0) as score
        FROM questions q
        LEFT JOIN submissions s ON s.question_id = q.id AND s.user_id = %s
        ORDER BY q.id
    """, (session["user_id"],))
    
    rows = cur.fetchall()
    total = sum(r[2] for r in rows if r[2] and r[1] != 'NO ATTEMPT') if rows else 0
    
    conn.close()
    
    return render_template("report.html", rows=rows, total=total)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
