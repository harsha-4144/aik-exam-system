import sqlite3
from werkzeug.security import generate_password_hash

conn = sqlite3.connect("exam.db")
cur = conn.cursor()

# FORCE DROP (DEV RESET)
cur.execute("DROP TABLE IF EXISTS submissions")
cur.execute("DROP TABLE IF EXISTS test_cases")
cur.execute("DROP TABLE IF EXISTS questions")
cur.execute("DROP TABLE IF EXISTS users")

# USERS - ADD PASSWORD COLUMN
cur.execute("""
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    password TEXT
)
""")

# QUESTIONS (WITH MARKS)
cur.execute("""
CREATE TABLE questions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    description TEXT,
    marks INTEGER,
    time_limit INTEGER DEFAULT 1800  -- Default 30 minutes in seconds
)
""")

# TEST CASES - ADD is_example COLUMN
cur.execute("""
CREATE TABLE test_cases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    question_id INTEGER,
    input TEXT,
    expected_output TEXT,
    is_example INTEGER DEFAULT 0
)
""")

# SUBMISSIONS
cur.execute("""
CREATE TABLE submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    question_id INTEGER,
    code TEXT,
    verdict TEXT,
    score INTEGER,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

# SEED DATA
questions = [
    (
        "Sum of Two Numbers",
        "Read two integers from input and print their sum.",
        10,
        900,  # 15 minutes
        [("2 3", "5", 1), ("10 20", "30", 0), ("-5 8", "3", 0)]
    ),
    (
        "Square a Number",
        "Read an integer and print its square.",
        10,
        1200,  # 20 minutes
        [("3", "9", 1), ("-4", "16", 0), ("0", "0", 0)]
    )
]

# Create admin user
admin_password = generate_password_hash("admin123")
cur.execute(
    "INSERT OR IGNORE INTO users (username, password) VALUES (?, ?)",
    ("admin", admin_password)
)

for title, desc, marks, time_limit, tests in questions:
    cur.execute(
        "INSERT INTO questions (title, description, marks, time_limit) VALUES (?, ?, ?, ?)",
        (title, desc, marks, time_limit)
    )
    qid = cur.lastrowid

    for inp, out, is_example in tests:
        cur.execute(
            """INSERT INTO test_cases 
            (question_id, input, expected_output, is_example) 
            VALUES (?, ?, ?, ?)""",
            (qid, inp, out, is_example)
        )

conn.commit()
conn.close()

print("Database reset and initialized with proper schema.")
