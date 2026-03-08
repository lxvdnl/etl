from pymongo import MongoClient
import psycopg2

mongo_client = MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["etl_db"]

pg_conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="airflow",
    user="airflow",
    password="airflow"
)

cursor = pg_conn.cursor()

sessions_collection = mongo_db["UserSessions"]
events_collection = mongo_db["EventLogs"]
tickets_collection = mongo_db["SupportTickets"]
recommendations_collection = mongo_db["UserRecommendations"]
moderation_collection = mongo_db["ModerationQueue"]

cursor.execute("DELETE FROM moderation_flags")
cursor.execute("DELETE FROM moderation_queue")
cursor.execute("DELETE FROM recommended_products")
cursor.execute("DELETE FROM user_recommendations")
cursor.execute("DELETE FROM support_ticket_messages")
cursor.execute("DELETE FROM support_tickets")
cursor.execute("DELETE FROM event_logs")
cursor.execute("DELETE FROM session_pages")
cursor.execute("DELETE FROM session_actions")
cursor.execute("DELETE FROM user_sessions")

sessions = list(sessions_collection.find())

for session in sessions:
    session_id = session["session_id"]
    user_id = session["user_id"]
    start_time = session["start_time"]
    end_time = session["end_time"]
    device = session["device"]

    cursor.execute(
        """
        INSERT INTO user_sessions (session_id, user_id, start_time, end_time, device)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (session_id, user_id, start_time, end_time, device)
    )

    for idx, page in enumerate(session.get("pages_visited", []), start=1):
        cursor.execute(
            """
            INSERT INTO session_pages (session_id, page, page_order)
            VALUES (%s, %s, %s)
            """,
            (session_id, page, idx)
        )

    for idx, action in enumerate(session.get("actions", []), start=1):
        cursor.execute(
            """
            INSERT INTO session_actions (session_id, action, action_order)
            VALUES (%s, %s, %s)
            """,
            (session_id, action, idx)
        )

events = list(events_collection.find())

for event in events:
    event_id = event["event_id"]
    event_time = event["timestamp"]
    event_type = event["event_type"]

    details = event.get("details", {})
    page = details.get("page")
    element = details.get("element")

    cursor.execute(
        """
        INSERT INTO event_logs (event_id, event_time, event_type, page, element)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (event_id, event_time, event_type, page, element)
    )

tickets = list(tickets_collection.find())

for ticket in tickets:
    ticket_id = ticket["ticket_id"]
    user_id = ticket["user_id"]
    status = ticket["status"]
    issue_type = ticket["issue_type"]
    created_at = ticket["created_at"]
    updated_at = ticket["updated_at"]

    cursor.execute(
        """
        INSERT INTO support_tickets (ticket_id, user_id, status, issue_type, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (ticket_id, user_id, status, issue_type, created_at, updated_at)
    )

    for message in ticket.get("messages", []):
        sender = message["sender"]
        text = message["message"]
        timestamp = message["timestamp"]

        cursor.execute(
            """
            INSERT INTO support_ticket_messages (ticket_id, sender, message, message_time)
            VALUES (%s, %s, %s, %s)
            """,
            (ticket_id, sender, text, timestamp)
        )

recommendations = list(recommendations_collection.find())

for recommendation in recommendations:
    user_id = recommendation["user_id"]
    last_updated = recommendation["last_updated"]

    cursor.execute(
        """
        INSERT INTO user_recommendations (user_id, last_updated)
        VALUES (%s, %s)
        """,
        (user_id, last_updated)
    )

    for idx, product_id in enumerate(recommendation.get("recommended_products", []), start=1):
        cursor.execute(
            """
            INSERT INTO recommended_products (user_id, product_id, product_order)
            VALUES (%s, %s, %s)
            """,
            (user_id, product_id, idx)
        )

moderation_items = list(moderation_collection.find())

for item in moderation_items:
    review_id = item["review_id"]
    user_id = item["user_id"]
    product_id = item["product_id"]
    review_text = item["review_text"]
    rating = item["rating"]
    moderation_status = item["moderation_status"]
    submitted_at = item["submitted_at"]

    cursor.execute(
        """
        INSERT INTO moderation_queue (
            review_id, user_id, product_id, review_text, rating, moderation_status, submitted_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (review_id, user_id, product_id, review_text, rating, moderation_status, submitted_at)
    )

    for flag in item.get("flags", []):
        cursor.execute(
            """
            INSERT INTO moderation_flags (review_id, flag)
            VALUES (%s, %s)
            """,
            (review_id, flag)
        )

pg_conn.commit()
cursor.close()
pg_conn.close()

print("Replication completed")
print("UserSessions:", len(sessions))
print("EventLogs:", len(events))
print("SupportTickets:", len(tickets))
print("UserRecommendations:", len(recommendations))
print("ModerationQueue:", len(moderation_items))