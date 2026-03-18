import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


def process_batch(batch_df, batch_id):
    """This function runs every 10 seconds for each new micro-batch"""
    print(f"Processing Batch ID: {batch_id}")

    # Action 1: Show in Console (so you can see it's working)
    batch_df.show(truncate=False)

    # Action 2: Write to Parquet (Persistence)
    # Note: Use .write (batch) instead of .writeStream inside here
    (
        batch_df.write.format("parquet")
        .mode("append")
        .partitionBy("processing_date")
        .save("data/output/raw_fishing_data")
    )


def main():
    spark_ver = pyspark.__version__
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_ver}"

    print(f"Using Kafka Package: {kafka_package}")

    spark = (
        SparkSession.builder.appName("FishingDataConsumer")
        .config("spark.jars.packages", kafka_package)
        .config("spark.sql.streaming.checkpointLocation", "checkpoints/")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("year", IntegerType(), True),
            StructField("scientific_name", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("catch_sum", DoubleType(), True),
            StructField("real_value", DoubleType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )

    # Read the stream from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "fishing_records")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Process the binary data into structured columns
    structured_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("ts", to_timestamp(col("timestamp")))  # Convert string to TS
        .withColumn("processing_date", to_date(col("ts")))  # Extract Date
    )

    # Output to paraquet sink
    query = (
        structured_df.writeStream.foreachBatch(
            process_batch
        ) 
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
