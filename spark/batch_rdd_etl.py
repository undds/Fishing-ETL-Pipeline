"""
Load the raw Parquet data as an RDD.
Use RDD transformations (map, filter, reduceByKey) to:
Filter out CANCELLED orders.
Compute total revenue per product_id using key-value pair RDDs.
Save the result as a text file.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FishingRDD_ETL").getOrCreate()
sc = spark.sparkContext

df = spark.read.parquet("data/output/raw_fishing_data")

df.printSchema()

rdd = df.rdd

scientific_revenue = (
    rdd.map(lambda row: (
        row.scientific_name,
        float(row.real_value) if row.real_value else 0.0
    ))
    .reduceByKey(lambda a, b: a + b)
)

top_scientific = scientific_revenue.takeOrdered(10, key=lambda x: -x[1])

print("\n=== Top Scientific Names by Revenue ===")
for name, revenue in top_scientific:
    print(f"{name}: ${revenue:.2f}")

scientific_revenue.saveAsTextFile("data/output/rdd_scientific_revenue")