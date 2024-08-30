from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext.getOrCreate()

sales_data = [("product1", 10), ("product2", 20), ("product1", 30), 
              ("product2", 10), ("product1", 5), ("product3", 40)]
# Create an RDD with sales data
sales_rdd = sc.parallelize(sales_data)

# Use reduceByKey to sum sales for each product
result_reduceByKey = sales_rdd.reduceByKey(lambda x, y: x + y).collect()

# Print result
print("reduceByKey result: ", result_reduceByKey)

# Stop the SparkContext
sc.stop()
