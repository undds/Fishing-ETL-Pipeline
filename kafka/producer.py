from kafka import KafkaProducer
import json
import time
from datetime import datetime
from faker import Faker
import random
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap-servers", default="localhost:9092")
args, _ = parser.parse_known_args()

ENTITIES = [
    "Japan",
    "Germany",
    "United States",
    "United Kingdom",
    "France",
    "Italy",
]

FISH_SCIENTIFIC = [
    "Thunnus albacares",
    "Thunnus obesus",
    "Thunnus alalunga",
    "Thunnus thynnus",
    "Xiphias gladius"
]

SECTORS = [
    "Industrial",
    "Artisanal",
    "Recreational",
    "Subsistence",
]

CATCH_SUMS = [1000, 2000, 3000, 4000, 5000]
REAL_SUMS = [0.5, 1000, 300, 700, 32, 100, 21, -1]
MIN_EVENTS = 500


fake = Faker()

def generate_random_record():
    return {
        "year": int(fake.year()),
        "scientific_name": fake.random.choice(FISH_SCIENTIFIC),
        "entity": fake.random.choice(ENTITIES),
        "sector": fake.random.choice(SECTORS),
        "catch_sum": float(fake.random.choice(CATCH_SUMS)),
        "real_value": float(fake.random.choice(REAL_SUMS)),
        "timestamp": datetime.now().isoformat(),
    }

def create_producer(bootstrap_servers: str = "localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        api_version=(3, 5, 0),
        compression_type="lz4",
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=30000,
        retries=5,
    )

total_sent = 0

def on_success(record_metadata):
    global total_sent
    total_sent += 1
    if total_sent % 100 == 0:
        print(f"Sent {total_sent} records to {record_metadata.topic}")

def on_error(excp):
    print(f"Failed to send record: {excp}")

def send_records(topic: str = "fishing_records", run_length: int = 30):
    producer = create_producer()
    start_time = time.perf_counter()

    print(f"Sending records to topic '{topic}' for {run_length} seconds...")

    try:
        for _ in range(MIN_EVENTS):
            if time.perf_counter() - start_time > run_length:
                break

            record = generate_random_record()

            producer.send(topic=topic, value=record) \
                .add_callback(on_success) \
                .add_errback(on_error)

            print(f"Sent: {record}")
            time.sleep(0.05)

    except KeyboardInterrupt:
        print("Stopping producer")
    finally:
        producer.flush()

def main():
    print("Starting Kafka Producer")
    time.sleep(5)  # allow Kafka to be ready
    send_records()
    print(f"Finished. Total records sent: {total_sent}")

if __name__ == "__main__":
    main()
