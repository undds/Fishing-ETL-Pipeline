"""
Load the raw Parquet data as an RDD.
Use RDD transformations (map, filter, reduceByKey) to:
Filter out CANCELLED orders.
Compute total revenue per product_id using key-value pair RDDs.
Save the result as a text file.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FishingRDD_ETL") \
    .getOrCreate()

sc = spark.sparkContext

# Load raw parquet
df = spark.read.parquet("data/raw/orders")

rdd = df.rdd

# -----------------------------------
# Filter out CANCELLED events
# (use catch_status as event type)
# -----------------------------------
valid_records = rdd.filter(lambda row: row.catch_status != "Discards")

# -----------------------------------
# Revenue per gear_name (product)
# -----------------------------------
gear_revenue = valid_records \
    .map(lambda row: (row.gear_name, float(row.real_value))) \
    .reduceByKey(lambda a,b: a+b)

# -----------------------------------
# Top selling gear
# -----------------------------------
top_gear = gear_revenue.takeOrdered(10, key=lambda x: -x[1])

print("Top Gear Revenue:")
for g in top_gear:
    print(g)

# -----------------------------------
# Save output
# -----------------------------------
gear_revenue.saveAsTextFile("data/output/rdd_gear_revenue")