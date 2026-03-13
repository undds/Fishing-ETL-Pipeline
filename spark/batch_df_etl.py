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
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("FishingDataFrameETL") \
    .getOrCreate()

# -----------------------------------
# Load raw data
# -----------------------------------
orders = spark.read.parquet("data/raw/orders")

orders.cache()

# -----------------------------------
# Hourly Sales Summary
# (using year since dataset lacks timestamp)
# -----------------------------------
hourly_sales = orders \
    .groupBy("year") \
    .agg(
        count("*").alias("total_records"),
        sum("real_value").alias("total_revenue"),
        avg("real_value").alias("avg_value")
    )

hourly_sales.write.mode("overwrite") \
    .partitionBy("year") \
    .parquet("data/output/yearly_summary")

# -----------------------------------
# Top 10 Products (gear_name)
# -----------------------------------
product_sales = orders \
    .groupBy("gear_name") \
    .agg(sum("catch_sum").alias("total_catch"))

windowSpec = Window.orderBy(desc("total_catch"))

top_products = product_sales \
    .withColumn("rank", rank().over(windowSpec)) \
    .filter(col("rank") <= 10)

top_products.write.mode("overwrite") \
    .parquet("data/output/top_gear")

# -----------------------------------
# Regional Revenue
# -----------------------------------
regions = spark.read.csv("regions.csv", header=True)

regional_revenue = orders \
    .join(regions, orders.fishing_entity == regions.entity) \
    .groupBy("region") \
    .agg(sum("real_value").alias("total_revenue"))

regional_revenue.write.mode("overwrite") \
    .partitionBy("region") \
    .parquet("data/output/regional_revenue")

# -----------------------------------
# Catch Status Breakdown (Pivot)
# -----------------------------------
status_breakdown = orders \
    .groupBy("fishing_entity") \
    .pivot("catch_status") \
    .count()

status_breakdown.write.mode("overwrite") \
    .parquet("data/output/status_breakdown")