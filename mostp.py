from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create Spark session
spark = SparkSession.builder.appName("Most Popular Genre").getOrCreate()

# Read the Parquet data
parquet_path = "gs://pysparkbucket343/output/movielens_parquet"
df = spark.read.parquet(parquet_path)

# Split genres into separate rows and count the occurrences of each genre
genres_df = df.select(explode(split(df["genres"], "\\|")).alias("genre"))
popular_genre_df = genres_df.groupBy("genre").count().orderBy("count", ascending=False)

# Show the results
popular_genre_df.show()

# Stop the Spark session
spark.stop()
