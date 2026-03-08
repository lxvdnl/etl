from pymongo import MongoClient
from faker import Faker
import random
from datetime import timedelta

fake = Faker()

client = MongoClient("mongodb://mongodb:27017/")
db = client["etl_db"]

sessions_collection = db["UserSessions"]
events_collection = db["EventLogs"]
tickets_collection = db["SupportTickets"]
recommendations_collection = db["UserRecommendations"]
moderation_collection = db["ModerationQueue"]

sessions_collection.delete_many({})
events_collection.delete_many({})
tickets_collection.delete_many({})
recommendations_collection.delete_many({})
moderation_collection.delete_many({})

user_ids = [f"user_{i}" for i in range(1, 101)]
product_ids = [f"prod_{i}" for i in range(1, 201)]

sessions = []
for i in range(500):
    start = fake.date_time_this_year()
    end = start + timedelta(minutes=random.randint(1, 60))
    sessions.append({
        "session_id": f"sess_{i}",
        "user_id": random.choice(user_ids),
        "start_time": start,
        "end_time": end,
        "pages_visited": random.sample(
            ["/home", "/catalog", "/product/1", "/product/2", "/cart", "/profile"],
            k=random.randint(2, 5)
        ),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "actions": random.sample(
            ["login", "view_product", "add_to_cart", "checkout", "logout"],
            k=random.randint(2, 5)
        )
    })

if sessions:
    sessions_collection.insert_many(sessions)

events = []
for i in range(1000):
    events.append({
        "event_id": f"evt_{i}",
        "timestamp": fake.date_time_this_year(),
        "event_type": random.choice(["click", "view", "purchase", "search"]),
        "details": {
            "page": random.choice(["/home", "/catalog", "/product/1", "/cart"]),
            "element": random.choice(["button", "link", "image", "input"])
        }
    })

if events:
    events_collection.insert_many(events)

tickets = []
for i in range(200):
    created = fake.date_time_this_year()
    updated = created + timedelta(hours=random.randint(1, 72))
    tickets.append({
        "ticket_id": f"ticket_{i}",
        "user_id": random.choice(user_ids),
        "status": random.choice(["open", "closed", "in_progress"]),
        "issue_type": random.choice(["payment", "delivery", "account", "refund"]),
        "messages": [
            {
                "sender": "user",
                "message": fake.sentence(),
                "timestamp": created
            },
            {
                "sender": "support",
                "message": fake.sentence(),
                "timestamp": updated
            }
        ],
        "created_at": created,
        "updated_at": updated
    })

if tickets:
    tickets_collection.insert_many(tickets)

recommendations = []
for user_id in user_ids:
    recommendations.append({
        "user_id": user_id,
        "recommended_products": random.sample(product_ids, k=3),
        "last_updated": fake.date_time_this_year()
    })

if recommendations:
    recommendations_collection.insert_many(recommendations)

moderation_items = []
for i in range(300):
    moderation_items.append({
        "review_id": f"rev_{i}",
        "user_id": random.choice(user_ids),
        "product_id": random.choice(product_ids),
        "review_text": fake.text(max_nb_chars=120),
        "rating": random.randint(1, 5),
        "moderation_status": random.choice(["pending", "approved", "rejected"]),
        "flags": random.sample(
            ["contains_links", "contains_images", "spam_suspected", "offensive_language"],
            k=random.randint(0, 2)
        ),
        "submitted_at": fake.date_time_this_year()
    })

if moderation_items:
    moderation_collection.insert_many(moderation_items)

print("Data successfully inserted into MongoDB")
print("UserSessions:", sessions_collection.count_documents({}))
print("EventLogs:", events_collection.count_documents({}))
print("SupportTickets:", tickets_collection.count_documents({}))
print("UserRecommendations:", recommendations_collection.count_documents({}))
print("ModerationQueue:", moderation_collection.count_documents({}))