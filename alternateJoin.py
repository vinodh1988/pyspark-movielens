from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Join Example").getOrCreate()

# Create Product DataFrame
product_data = [(1, "Product A"), (2, "Product B")]
product_df = spark.createDataFrame(product_data, ["product_id", "product_name"])

# Create Sales DataFrame (same as above)
sales_data = [(1, 100, "2024-01-01"), (1, 200, "2024-01-02"),
              (2, 150, "2024-01-01"), (2, 250, "2024-01-03")]
sales_df = spark.createDataFrame(sales_data, ["product_id", "amount", "date"])

# Perform an inner join to get product names along with sales data
joined_df = sales_df.join(product_df, on="product_id", how="inner")

# Show the result
joined_df.show()

# Stop the session
spark.stop()
