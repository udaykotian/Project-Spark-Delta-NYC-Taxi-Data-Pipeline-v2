from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Define the SparkSession builder with Delta configurations
builder = SparkSession.builder \
    .appName("ExploreDeltaTable") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Configure the session with Delta Lake support
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Load the Delta table
delta_path = "delta/yellow_taxi_2023_01"
df = spark.read.format("delta").load(delta_path)

# Explore the data
print(f"Row count: {df.count()}")
df.printSchema()
df.show(5)  # Show first 5 rows

# Stop the Spark session
spark.stop()