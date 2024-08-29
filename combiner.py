from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("MovieLens Join") \
    .getOrCreate()

# Define the GCP bucket path
bucket = "gs://pysparkbucket343/ml-20m/"
output_path = "gs://pysparkbucket343/output/"

# File paths for MovieLens CSVs
movies_csv = bucket + "movies.csv"
ratings_csv = bucket + "ratings.csv"
tags_csv = bucket + "tags.csv"
links_csv = bucket + "links.csv"

# Read CSV files
movies_df = spark.read.option("header", "true").csv(movies_csv)
ratings_df = spark.read.option("header", "true").csv(ratings_csv)
tags_df = spark.read.option("header", "true").csv(tags_csv)
links_df = spark.read.option("header", "true").csv(links_csv)

# Convert columns to appropriate data types if needed
ratings_df = ratings_df.withColumn("userId", col("userId").cast("int")) \
                       .withColumn("movieId", col("movieId").cast("int")) \
                       .withColumn("rating", col("rating").cast("float"))

tags_df = tags_df.withColumn("userId", col("userId").cast("int")) \
                 .withColumn("movieId", col("movieId").cast("int"))

links_df = links_df.withColumn("movieId", col("movieId").cast("int"))

# Remove duplicate columns by renaming them appropriately during joins

# Join movies with ratings, no column name clashes
movies_ratings_df = movies_df.join(ratings_df, "movieId", "inner")

# Join the result with tags, renaming duplicate columns to avoid conflicts
movies_ratings_tags_df = movies_ratings_df.join(
    tags_df.withColumnRenamed("tag", "movie_tag").withColumnRenamed("timestamp", "tag_timestamp"),
    ["movieId", "userId"],
    "left_outer"
)

# Join the result with links, renaming duplicate columns as necessary
final_df = movies_ratings_tags_df.join(
    links_df.withColumnRenamed("imdbId", "imdb_id").withColumnRenamed("tmdbId", "tmdb_id"),
    "movieId",
    "left_outer"
)

# Write the final DataFrame to the output folder in Parquet format
final_df.write.mode("overwrite").parquet(output_path + "movielens_parquet")

# Stop the Spark session
spark.stop()

