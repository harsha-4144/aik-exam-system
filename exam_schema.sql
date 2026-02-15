-- Required synchronized exam tables
CREATE TABLE IF NOT EXISTS exam_sessions (
    id SERIAL PRIMARY KEY,
    session_name TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_minutes INTEGER NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS exam_participants (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES exam_sessions(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    ready_status BOOLEAN DEFAULT FALSE,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS exam_progress (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    question_id INTEGER NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
    status TEXT DEFAULT 'in_progress',
    score INTEGER DEFAULT 0,
    time_spent INTEGER DEFAULT 0
);

-- Extra columns used by synchronized flow
ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS question_ids TEXT DEFAULT '';
ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS is_paused BOOLEAN DEFAULT FALSE;
ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS paused_at TIMESTAMP;
ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS participant_limit INTEGER DEFAULT 0;
ALTER TABLE exam_sessions ADD COLUMN IF NOT EXISTS auto_start_when_all_ready BOOLEAN DEFAULT FALSE;
ALTER TABLE questions ADD COLUMN IF NOT EXISTS display_order INTEGER;
UPDATE questions
SET display_order = id
WHERE display_order IS NULL;

ALTER TABLE exam_progress ADD COLUMN IF NOT EXISTS session_id INTEGER REFERENCES exam_sessions(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS ux_exam_participants_session_user
    ON exam_participants(session_id, user_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_exam_progress_session_user_question
    ON exam_progress(session_id, user_id, question_id);

-- Global toggle storage for questions page visibility
CREATE TABLE IF NOT EXISTS exam_settings (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL
);

INSERT INTO exam_settings(setting_key, setting_value)
VALUES ('questions_hidden', '0')
ON CONFLICT (setting_key) DO NOTHING;

-- Per-submission test case results (admin reporting)
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
