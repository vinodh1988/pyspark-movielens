from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext.getOrCreate()

sales_data = [("product1", 10), ("product2", 20), ("product1", 30), 
              ("product2", 10), ("product1", 5), ("product3", 40)]
# Create an RDD with sales data
sales_rdd = sc.parallelize(sales_data)

# Use groupByKey to group sales for each product
result_groupByKey = sales_rdd.groupByKey().mapValues(lambda x: sum(x)).collect()

# Print result
print("groupByKey result: ", result_groupByKey)

# Stop the SparkContext
sc.stop()
