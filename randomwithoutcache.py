from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

# Initialize Spark session
spark = SparkSession.builder.appName("Non-Cache General Example").getOrCreate()

# Create a large synthetic dataset with 10 million rows
data = spark.range(0, 10000000).withColumn("random_value", rand())

# Perform a series of transformations
transformed_data = data.filter(data["random_value"] > 0.5).select("id", "random_value")

# Perform multiple actions without caching
count_non_cached = transformed_data.count()  # First action triggers computation
print(f"Count without cache: {count_non_cached}")

# Perform another action without caching
sum_non_cached = transformed_data.selectExpr("sum(random_value)").collect()  # Recomputes the data
print(f"Sum without cache: {sum_non_cached}")

# Stop the session
spark.stop()
