from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create Spark session
spark = SparkSession.builder.appName("Average Rating Per Movie").getOrCreate()

# Read the Parquet data
parquet_path = "gs://pysparkbucket343/output/movielens_parquet"
df = spark.read.parquet(parquet_path)

# Group by movieId and calculate the average rating
avg_ratings_df = df.groupBy("movieId").agg(avg("rating").alias("average_rating"))

# Show the results
avg_ratings_df.show()

# Stop the Spark session
spark.stop()
