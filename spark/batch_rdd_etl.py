"""
Load the raw Parquet data as an RDD.
Use RDD transformations (map, filter, reduceByKey) to:
Filter out CANCELLED orders.
Compute total revenue per product_id using key-value pair RDDs.
Save the result as a text file.
"""
