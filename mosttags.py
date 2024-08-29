from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Users with Most Tags").getOrCreate()

# Read the Parquet data
parquet_path = "gs://pysparkbucket343/output/movielens_parquet"
df = spark.read.parquet(parquet_path)

# Group by userId and count the number of tags
tags_per_user_df = df.groupBy("userId").count().withColumnRenamed("count", "tags_count") \
    .orderBy("tags_count", ascending=False)

# Show the results
tags_per_user_df.show()

# Stop the Spark session
spark.stop()
