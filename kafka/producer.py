import json
import time
from kafka import KafkaProducer

# Path to your big JSON file
DATA_FILE = "/media/abhay/768270A98270700B/bigdata/churn_streaming_project/data/data.json"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "music_events"

print("Starting Kafka producer...")

with open(DATA_FILE, "r", encoding="utf-8") as f:
    for i, line in enumerate(f):
        line = line.strip()
        if not line:
            continue

        try:
            record = json.loads(line)
            producer.send(topic, value=record)

            if i % 1000 == 0:
                print(f"Sent {i} records")

            # simulate streaming
            time.sleep(0.01)

        except Exception as e:
            print("Error:", e)

producer.flush()
producer.close()

print("Finished sending data.")
