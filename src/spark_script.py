from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *

# Initialize Spark session with Delta Lake support
builder = SparkSession.builder \
    .appName("NYC Taxi Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = builder.getOrCreate()  # Corrected line

# Define file paths (using absolute paths based on your project structure)
trip_data_path = "/Users/udayag/spark-delta-taxi-pipeline/data/yellow_tripdata_2023-01.parquet"
zone_lookup_path = "/Users/udayag/spark-delta-taxi-pipeline/data/taxi_zone_lookup.csv"
delta_path = "/Users/udayag/spark-delta-taxi-pipeline/data/delta/taxi_trips"

# Load data
print("Loading data...")
trip_df = spark.read.parquet(trip_data_path)
zone_df = spark.read.option("header", "true").csv(zone_lookup_path)

# Clean data
print("Cleaning data...")
trip_df = trip_df.dropna(subset=["PULocationID", "DOLocationID"]) \
                 .withColumn("PULocationID", col("PULocationID").cast("integer")) \
                 .withColumn("DOLocationID", col("DOLocationID").cast("integer"))
zone_df = zone_df.withColumn("LocationID", col("LocationID").cast("integer"))

# Transform data
print("Transforming data...")
joined_df = trip_df.join(broadcast(zone_df), trip_df.PULocationID == zone_df.LocationID, "left")
processed_df = joined_df.select(
    to_date("tpep_pickup_datetime").alias("pickup_date"),
    col("Borough").alias("pickup_borough"),
    "fare_amount"
)
aggregated_df = processed_df.groupBy("pickup_date", "pickup_borough") \
                            .agg(
                                sum("fare_amount").alias("total_fares"),
                                count("*").alias("trip_count")
                            )

# Write to Delta Lake
print("Writing to Delta Lake...")
aggregated_df.write \
             .format("delta") \
             .mode("overwrite") \
             .partitionBy("pickup_date") \
             .save(delta_path)

print("Done.")
spark.stop()