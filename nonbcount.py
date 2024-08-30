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

# Get the count of rows after the join
record_count = non_broadcast_join.count()

# Print the result
print(f"Non-Broadcast Join Count: {record_count}")

# Stop the Spark session
spark.stop()
