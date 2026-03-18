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
    col,
    sum,
    avg,
    count,
    hour,
    to_timestamp,
    row_number,
    broadcast
)
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder \
        .appName("FishingBatchETL") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------------
    # Load Data (Streaming Output)
    # -----------------------------------
    df = spark.read.parquet("data/output/raw_fishing_data")

    # Handle nulls
    df = df.fillna({"real_value": 0.0})

    # Convert timestamp
    df = df.withColumn("ts", to_timestamp(col("timestamp")))

    # Cache base DataFrame
    df.cache()

    # -----------------------------------
    # 1. Hourly Summary
    # -----------------------------------
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
    hourly_summary.show(truncate=False)

    # -----------------------------------
    # 2. Top 10 Products (Window Function)
    # (Partitioned to avoid Spark warning)
    # -----------------------------------
    window_spec = Window.partitionBy("entity").orderBy(col("total_value").desc())

    top_products = (
        df.groupBy("entity", "scientific_name")
        .agg(sum("real_value").alias("total_value"))
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= 10)
    )

    print("\n=== Top Products per Entity ===")
    top_products.show(truncate=False)

    # -----------------------------------
    # 3. Regional Revenue (WITH CSV JOIN)
    # -----------------------------------

    # Load regions lookup table
    regions_df = spark.read.csv(
        "data/regions.csv",
        header=True,
        inferSchema=True
    )

    # Broadcast (small dataset optimization)
    regions_df = broadcast(regions_df)

    # Join with main dataset
    enriched_df = df.join(
        regions_df,
        on="entity",
        how="left"
    )

    # Aggregate by region
    regional_revenue = (
        enriched_df.groupBy("region")
        .agg(sum("real_value").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )

    print("\n=== Regional Revenue by Region ===")
    regional_revenue.show(truncate=False)

    # -----------------------------------
    # 4. Sector Breakdown (Pivot)
    # -----------------------------------
    sector_breakdown = (
        df.groupBy("entity")
        .pivot("sector")
        .count()
    )

    print("\n=== Sector Breakdown ===")
    sector_breakdown.show(truncate=False)

    # -----------------------------------
    # Write Outputs
    # -----------------------------------

    hourly_summary.write \
        .mode("overwrite") \
        .partitionBy("hour") \
        .parquet("data/output/hourly_summary")

    top_products.write \
        .mode("overwrite") \
        .parquet("data/output/top_products")

    regional_revenue.write \
        .mode("overwrite") \
        .parquet("data/output/regional_revenue_by_region")

    sector_breakdown.write \
        .mode("overwrite") \
        .parquet("data/output/sector_breakdown")

    print("\n✅ Batch ETL Job Completed Successfully")

    spark.stop()


if __name__ == "__main__":
    main()