from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Count Ratings Per Movie").getOrCreate()

# Read the Parquet data
parquet_path = "gs://pysparkbucket343/output/movielens_parquet"
df = spark.read.parquet(parquet_path)

# Group by movieId and count the number of ratings
ratings_count_df = df.groupBy("movieId").count().withColumnRenamed("count", "ratings_count")

# Show the results
ratings_count_df.show()

# Stop the Spark session
spark.stop()

