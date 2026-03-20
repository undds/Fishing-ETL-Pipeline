"""

Load the raw Parquet data into a Spark DataFrame.
Perform the following transformations:
Hourly Sales Summary — Group by hour, compute total_orders, total_revenue, avg_order_value.
Top 10 Products — Rank products by total quantity sold using Spark SQL window functions.
Regional Revenue — Join orders with a static regions.csv reference dataset to enrich region names, then aggregate revenue by region.
Order Status Breakdown — Pivot on order_status to get counts per category.
Write each output to Parquet, partitioned and bucketed where appropriate.
Use caching on the base DataFrame to speed up multiple downstream transformations.

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, hour, to_timestamp, row_number, broadcast
)
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder.appName("FishingBatchETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet("/opt/project/data/output/raw_fishing_data")

    df = df.fillna({"real_value": 0.0})
    df = df.withColumn("ts", to_timestamp(col("timestamp")))

    df.cache()

    negative_values_df = df.filter(col("real_value") < 0)

    # Get the count for logging purposes
    neg_count = negative_values_df.count()
    print(f"\n=== Quality Check ===")
    print(f"Number of rows with negative 'real_value': {neg_count}")

    # Write the negative records to Parquet for investigation
    negative_values_df.write.mode("overwrite").parquet(
        "data/output/negative_values_report"
    )

    # Hourly Summary
    hourly_summary = (
        df.withColumn("hour", hour(col("ts")))
        .groupBy("hour")
        .agg(
            count("*").alias("total_records"),
            sum("real_value").alias("total_revenue"),
            avg("real_value").alias("avg_value")
        )
    )

    print("\n=== Hourly Summary ===")
    hourly_summary.show()

    # Top Products per Entity
    window_spec = Window.partitionBy("entity").orderBy(col("total_value").desc())

    top_products = (
        df.groupBy("entity", "scientific_name")
        .agg(sum("real_value").alias("total_value"))
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= 10)
    )

    print("\n=== Top Products per Entity ===")
    top_products.show()

    # Sector Breakdown
    sector_breakdown = (
        df.groupBy("entity")
        .pivot("sector")
        .count()
    )

    print("\n=== Sector Breakdown ===")
    sector_breakdown.show()

    # Write outputs
    hourly_summary.write.mode("overwrite").partitionBy("hour").parquet("data/output/hourly_summary")
    top_products.write.mode("overwrite").parquet("data/output/top_products")
    sector_breakdown.write.mode("overwrite").parquet("data/output/sector_breakdown")

    print("\nBatch ETL Completed Successfully")

    spark.stop()

if __name__ == "__main__":
    main()
