from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col
import time

def main():
    """
    Test query performance on the optimized Delta table.
    """
    # Set up Spark session with Delta Lake support
    builder = (
        SparkSession.builder
        .appName("TestQueryPerformance")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Load the optimized Delta table
    df = spark.read.format("delta").load("delta/yellow_taxi_2023_01_transformed")

    # Measure query performance
    start_time = time.time()
    count = df.filter(col("PULocationID") == 161).count()
    end_time = time.time()

    # Print results
    print(f"Query took {end_time - start_time:.2f} seconds. Count: {count}")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
    