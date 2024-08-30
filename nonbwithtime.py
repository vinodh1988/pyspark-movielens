from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Non-Broadcast Join Example") \
    .getOrCreate()

# Path to your dataset in Google Storage
bucket_path = "gs://pysparkbucket343/ml-20m/"

# Load the ratings and movies datasets
ratings = spark.read.csv(f"{bucket_path}ratings.csv", header=True, inferSchema=True)
movies = spark.read.csv(f"{bucket_path}movies.csv", header=True, inferSchema=True)

# Perform the non-broadcast join on movieId
non_broadcast_join = ratings.join(movies, ratings["movieId"] == movies["movieId"])

# Explain physical plan
non_broadcast_join.explain(True)

# Force execution to measure time
non_broadcast_join.count()

# Stop the Spark session
spark.stop()
