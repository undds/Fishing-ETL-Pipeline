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

spark = SparkSession.builder.appName("LoadCSV").getOrCreate()
file_path = "data/rfmo_12.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# group by year, sector_type, and common_name, and catch_sum
yearly_summary = df.groupBy("year", "sector_type", "common_name").sum("catch_sum")
yearly_summary.show()

# show top 10 products by real_value
top10_summary = df.groupBy("common_name").sum("real_value").orderBy("sum(real_value)", ascending=False).limit(10)
top10_summary.show()

# ordered by fishing_entity and year, and sum of real_value
regional_revenue = df.groupBy("fishing_entity", "year").sum("real_value").orderBy("fishing_entity", "year")
regional_revenue.show()

# count reporting_status
reporting_status_breakdown = df.groupBy("reporting_status").count()
reporting_status_breakdown.show()




df.show()

df.printSchema()


