from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

# Initialize Spark session
spark = SparkSession.builder.appName("Cache General Example").getOrCreate()

# Create a large synthetic dataset with 10 million rows
data = spark.range(0, 10000000).withColumn("random_value", rand())

# Perform a series of transformations
transformed_data = data.filter(data["random_value"] > 0.5).select("id", "random_value")

# Cache the transformed dataset
transformed_data.cache()  # Cache the data in memory

# Perform multiple actions with caching
count_cached = transformed_data.count()  # First action triggers computation and caches the data
print(f"Count with cache: {count_cached}")

# Perform another action with caching (should be faster since data is cached)
sum_cached = transformed_data.selectExpr("sum(random_value)").collect()  # Uses cached data
print(f"Sum with cache: {sum_cached}")

# Stop the session
spark.stop()
