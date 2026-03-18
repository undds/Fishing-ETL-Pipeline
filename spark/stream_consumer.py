import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
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
            StructField("rfmo_id", IntegerType(), True),
            StructField("rfmo_name", StringType(), True),
            StructField("layer_name", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("scientific_name", StringType(), True),
            StructField("common_name", StringType(), True),
            StructField("functional_group", StringType(), True),
            StructField("commercial_group", StringType(), True),
            StructField("fishing_entity", StringType(), True),
            StructField("sector_type", StringType(), True),
            StructField("catch_status", StringType(), True),
            StructField("reporting_status", StringType(), True),
            StructField("gear_name", StringType(), True),
            StructField("catch_sum", DoubleType(), True),
            StructField("real_value", DoubleType(), True),
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
    )

    # Output to console
    query = (
        structured_df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
