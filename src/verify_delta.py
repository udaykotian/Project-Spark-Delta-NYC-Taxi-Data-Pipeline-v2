from pyspark.sql import SparkSession
from delta import *

# Initialize Spark session with Delta Lake support, explicitly adding the package
builder = SparkSession.builder \
    .appName("Verify Delta Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .getOrCreate()

# Assign the session to the 'spark' variable
spark = builder

# Define the Delta table path
delta_path = "/Users/udayag/spark-delta-taxi-pipeline/data/delta/taxi_trips"

# Load and display the Delta table
print("Loading Delta table...")
df = spark.read.format("delta").load(delta_path)
df.show()

print("Done.")
spark.stop()