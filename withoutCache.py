from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Non-Cache Example").getOrCreate()

# Load datasets
bucket_path = "gs://pysparkbucket343/ml-20m/"
ratings = spark.read.csv(f"{bucket_path}ratings.csv", header=True, inferSchema=True)

# Perform a simple transformation: filter movies with a rating of 4.0 or higher
high_rated_movies = ratings.filter(ratings["rating"] >= 4.0)

# Count the number of high-rated movies (first action)
count_non_cached = high_rated_movies.count()  # Will trigger computation
print(f"Count without cache: {count_non_cached}")

# Show the first few rows (second action)
high_rated_movies.show(5)  # Will trigger re-computation

# Stop the session
spark.stop()
