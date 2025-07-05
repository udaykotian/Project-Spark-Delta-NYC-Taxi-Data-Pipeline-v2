from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, avg
import matplotlib.pyplot as plt

def main():
    """
    Perform advanced analysis to calculate average trip duration by borough and visualize it.
    """
    # Step 1: Set up Spark session with Delta Lake support
    builder = (
        SparkSession.builder
        .appName("AdvancedAnalysis")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Step 2: Define the Delta table path and register as a persistent table
    delta_path = "/Users/udayag/spark-delta-taxi-pipeline/delta/yellow_taxi_2023_01_transformed"
    spark.sql(f"CREATE TABLE IF NOT EXISTS yellow_taxi_transformed USING delta LOCATION '{delta_path}'")

    # Step 3: Load the Delta table
    df = spark.sql("SELECT * FROM yellow_taxi_transformed")

    # Step 4: Calculate average trip duration by borough
    print("Calculating average trip duration by borough...")
    avg_duration_df = df.groupBy("pickup_borough").agg(avg("trip_duration").alias("avg_trip_duration")).orderBy(col("avg_trip_duration").desc())
    avg_duration_df.show()

    # Step 5: Convert to Pandas for visualization
    pandas_df = avg_duration_df.toPandas()

    # Step 6: Create a bar plot
    plt.figure(figsize=(10, 6))
    plt.bar(pandas_df["pickup_borough"], pandas_df["avg_trip_duration"], color='skyblue')
    plt.xlabel("Borough")
    plt.ylabel("Average Trip Duration (minutes)")
    plt.title("Average Trip Duration by Borough")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # Step 7: Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()