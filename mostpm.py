from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create Spark session
spark = SparkSession.builder.appName("Top 10 Movies by Average Rating").getOrCreate()

# Read the Parquet data
parquet_path = "gs://pysparkbucket343/output/movielens_parquet"
df = spark.read.parquet(parquet_path)

# Group by movieId and calculate the average rating, then order by rating in descending order
top_movies_df = df.groupBy("movieId", "title").agg(avg("rating").alias("average_rating")) \
    .orderBy("average_rating", ascending=False) \
    .limit(10)

# Show the results
top_movies_df.show()

# Stop the Spark session
spark.stop()
