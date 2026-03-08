import psycopg2

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="airflow",
    user="airflow",
    password="airflow"
)

cursor = conn.cursor()

cursor.execute("DELETE FROM user_activity_mart")

cursor.execute("""
INSERT INTO user_activity_mart (
    user_id,
    sessions_count,
    avg_session_duration_minutes,
    total_pages_visited,
    total_actions
)
SELECT
    s.user_id,
    COUNT(DISTINCT s.session_id) AS sessions_count,
    AVG(EXTRACT(EPOCH FROM (s.end_time - s.start_time)) / 60.0) AS avg_session_duration_minutes,
    COUNT(DISTINCT p.id) AS total_pages_visited,
    COUNT(DISTINCT a.id) AS total_actions
FROM user_sessions s
LEFT JOIN session_pages p ON s.session_id = p.session_id
LEFT JOIN session_actions a ON s.session_id = a.session_id
GROUP BY s.user_id
""")

cursor.execute("DELETE FROM support_efficiency_mart")

cursor.execute("""
INSERT INTO support_efficiency_mart (
    issue_type,
    status,
    tickets_count,
    avg_resolution_hours
)
SELECT
    issue_type,
    status,
    COUNT(ticket_id) AS tickets_count,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0) AS avg_resolution_hours
FROM support_tickets
GROUP BY issue_type, status
""")

conn.commit()

cursor.close()
conn.close()

print("Data marts built successfully")