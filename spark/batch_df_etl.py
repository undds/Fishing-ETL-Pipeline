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
