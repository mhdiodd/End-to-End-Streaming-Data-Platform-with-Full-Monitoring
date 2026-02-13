import json
import time
import pandas as pd
from confluent_kafka import Producer
from datetime import datetime

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "events.raw"

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"Record successfully produced to "
            f"{msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )

# Create Kafka producer
producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 50,
        "retries": 5,
    }
)

# Load CSV file
df = pd.read_csv("events_raw.csv")
print(f"Loaded {len(df)} events from CSV")

# Stream events one by one (simulate real-time ingestion)
for index, row in df.iterrows():
    event = {
        "event_id": row["event_id"],
        "user_id": int(row["user_id"]),
        "service_type": row["service_type"],
        "action": row["action"],
        "amount": int(row["amount"]),
        "event_time": row["event_time"],
        "ingested_at": datetime.utcnow().isoformat(),
    }

    producer.produce(
        topic=TOPIC_NAME,
        value=json.dumps(event).encode("utf-8"),
        on_delivery=delivery_report,
    )

    producer.poll(0)
    print(f"Sent event {index + 1}: {event['event_id']}")

    time.sleep(0.5)

producer.flush()
print("All events sent successfully")
