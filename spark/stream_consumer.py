import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap-servers", default="localhost:9092")
parser.add_argument("--duration", type=int, default=60)
args, unknown = parser.parse_known_args()


def process_batch(batch_df, batch_id):
    output_path = "/opt/project/data/output/raw_fishing_data"
    print(f"Processing batch {batch_id}...")

    batch_df.write.mode("append").parquet(output_path)

    batch_df.show(5)


def main():
    spark_ver = pyspark.__version__
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_ver}"

    spark = (
        SparkSession.builder
        .appName("FishingDataConsumer")
        .config("spark.jars.packages", kafka_package)
        .config("spark.sql.streaming.checkpointLocation", "checkpoints/")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("scientific_name", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("catch_sum", DoubleType(), True),
        StructField("real_value", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ])

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", "fishing_records")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # WRITING TO BRONZE LAYER
    query = (
        parsed_df.writeStream.format("parquet")
        .option("path", "/opt/project/data/bronze/fishing_raw")  # Changed to bronze
        .option("checkpointLocation", "/opt/project/data/checkpoint/bronze_fishing")
        .start()
    )

    print(f"Stream will run for {args.duration} seconds")
    query.awaitTermination(timeout=args.duration)

    query.stop()
    spark.stop()
    print("Stream finished successfully. Exiting")

if __name__ == "__main__":
    main()
