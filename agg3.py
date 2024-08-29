from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Count Tags Per Movie").getOrCreate()

# Read the Parquet data
parquet_path = "gs://pysparkbucket343/output/movielens_parquet"
df = spark.read.parquet(parquet_path)

# Group by movieId and count the number of tags
tags_count_df = df.groupBy("movieId").count().withColumnRenamed("count", "tags_count")

# Show the results
tags_count_df.show()

# Stop the Spark session
spark.stop()
