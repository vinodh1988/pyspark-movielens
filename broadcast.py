from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Broadcast Join Example") \
    .getOrCreate()

# Path to your dataset in Google Storage
bucket_path = "gs://pysparkbucket343/ml-20m/"

# Load the ratings and movies datasets
ratings = spark.read.csv(f"{bucket_path}ratings.csv", header=True, inferSchema=True)
movies = spark.read.csv(f"{bucket_path}movies.csv", header=True, inferSchema=True)

# Perform the broadcast join on movieId
broadcast_join = ratings.join(broadcast(movies), ratings["movieId"] == movies["movieId"])

# Show a few results
broadcast_join.show(5)

# Stop the Spark session
spark.stop()
