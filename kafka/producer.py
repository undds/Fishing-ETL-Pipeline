# module 1
from kafka import KafkaProducer
import json
import time
from datetime import datetime


def create_producer(bootstrap_servers: str = "localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        compression_type="lz4",
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

total_sent = 0

def on_success(record_metadata):
    global total_sent
    total_sent += 1
    if total_sent % 1000 == 0:
        print(f"Successfully sent {total_sent} records to {record_metadata.topic}")


def on_error(excp):
    print(f"Failed to send record: {excp}")


def send_records(topic: str = "fishing_records", run_length: int = 20):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("FishingProducer").master("local[*]").getOrCreate()
    )

    file_path = "data/rfmo_12.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Convert DataFrame rows to a list of dictionaries
    # .collect() brings the data into Python memory as Row objects
    # .asDict() converts those Row objects to dictionaries
    records = [row.asDict() for row in df.collect()]

    producer = create_producer()
    start_time = time.perf_counter()

    print(f"Starting stream of {len(records)} rows to Kafka topic '{topic}' for up to {run_length} seconds")

    try:
        for record in records:
            # Check if we've exceeded our run_length
            if time.perf_counter() - start_time > run_length:
                break

            # Send the actual row from the CSV
            producer.send(topic=topic, value=record).add_callback(
                on_success
            ).add_errback(on_error)

            print(
                f"Sent row: {list(record.values())[10:15]}..."
            ) 

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        spark.stop()  # Clean up Spark


def main():
    # Call the actual logic function, not just the producer creator
    print("Starting producer")
    send_records(topic="fishing_records", run_length=30)
    print("Finished")
    print(f"Total records sent: {total_sent}")


if __name__ == "__main__":
    main()
