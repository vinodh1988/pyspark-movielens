from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum

# Initialize Spark session
spark = SparkSession.builder.appName("Window Functions Example").getOrCreate()

# Create Sales DataFrame
sales_data = [(1, 100, "2024-01-01"), (1, 200, "2024-01-02"),
              (2, 150, "2024-01-01"), (2, 250, "2024-01-03")]
sales_df = spark.createDataFrame(sales_data, ["product_id", "amount", "date"])

# Define a window specification
window_spec = Window.partitionBy("product_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add a running total column using the window function
sales_df_with_running_total = sales_df.withColumn("running_total", sum("amount").over(window_spec))

# Show the result
sales_df_with_running_total.show()

# Stop the session
spark.stop()
