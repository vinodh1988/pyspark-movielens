from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Cache Example").getOrCreate()

# Load datasets
bucket_path = "gs://pysparkbucket343/ml-20m/"
ratings = spark.read.csv(f"{bucket_path}ratings.csv", header=True, inferSchema=True)

# Perform a simple transformation: filter movies with a rating of 4.0 or higher
high_rated_movies = ratings.filter(ratings["rating"] >= 4.0)

# Cache the DataFrame
high_rated_movies.cache()  # Cache in memory

# Count the number of high-rated movies (first action)
count_cached = high_rated_movies.count()  # Will trigger computation and cache the result
print(f"Count with cache: {count_cached}")

# Show the first few rows (second action)
high_rated_movies.show(5)  # Will not trigger re-computation; uses cached data

# Stop the session
spark.stop()
