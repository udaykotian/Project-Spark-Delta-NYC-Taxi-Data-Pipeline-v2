from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import avg, hour, col, count

# Define the SparkSession builder with Delta configurations
builder = SparkSession.builder \
    .appName("AnalyzeYellowTaxiData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Configure the session with Delta Lake support
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load the transformed Delta table
transformed_path = "delta/yellow_taxi_2023_01_transformed"
df = spark.read.format("delta").load(transformed_path)

# Filter out extreme trip durations (e.g., > 120 minutes)
df_filtered = df.filter(col("trip_duration") <= 120)

# Analysis steps
# 1. Aggregate statistics: Average trip_duration by PULocationID
avg_duration_by_location = df_filtered.groupBy("PULocationID").agg(avg("trip_duration").alias("avg_trip_duration"))

# 2. Sort and show top 10 locations by average duration
result = avg_duration_by_location.orderBy(col("avg_trip_duration").desc()).limit(10)
result.show()

# 3. Identify peak hours: Group by hour of pickup
peak_hours = df_filtered.groupBy(hour("tpep_pickup_datetime").alias("pickup_hour")).agg(
    avg("trip_duration").alias("avg_trip_duration"),
    count("*").alias("trip_count")
).orderBy(col("trip_count").desc())
peak_hours.show()

# 4. Visualization: Open canvas for bar chart of top 10 locations
print("Opening canvas for visualization...")
# Collect data for canvas (convert to list for plotting)
canvas_data = result.collect()
location_ids = [row["PULocationID"] for row in canvas_data]
durations = [row["avg_trip_duration"] for row in canvas_data]
# Simple code to visualize (executed in canvas panel)
canvas_code = """
import matplotlib.pyplot as plt
plt.bar(location_ids, durations)
plt.xlabel('PULocationID')
plt.ylabel('Average Trip Duration (minutes)')
plt.title('Top 10 Locations by Average Trip Duration')
plt.show()
"""
# Execute canvas code (Grok 3 can open a canvas panel)
exec(canvas_code)  # This will trigger the canvas panel in the UI

# Stop the Spark session
spark.stop()