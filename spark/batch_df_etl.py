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
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, hour, to_timestamp, row_number, broadcast
)
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder.appName("FishingBatchETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # New silver_etl.py logic
    df = spark.read.parquet("/opt/project/data/bronze/fishing_raw")

    # Data cleaning and enrichment
    silver_df = df.fillna({"real_value": 0.0}) \
                .withColumn("ts", to_timestamp(col("timestamp"))) \
                .filter(col("real_value") >= 0) # Quality Gate

    silver_df.write.mode("overwrite").parquet("/opt/project/data/silver/fishing_cleaned")



    # GOLD LAYER
    # In spark/batch_df_etl.py
    df = spark.read.parquet("/opt/project/data/silver/fishing_cleaned")

    # Example Gold Table: Top Products
    top_products = (
        df.groupBy("entity", "scientific_name")
        .agg(sum("real_value").alias("total_value"))
    )
    top_products.write.mode("overwrite").parquet("/opt/project/data/gold/top_products_by_entity")

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
    top_products.write.mode("overwrite").parquet("data/output/top_products")
    sector_breakdown.write.mode("overwrite").parquet("data/output/sector_breakdown")

    print("\nBatch ETL Completed Successfully")

    spark.stop()

if __name__ == "__main__":
    main()
