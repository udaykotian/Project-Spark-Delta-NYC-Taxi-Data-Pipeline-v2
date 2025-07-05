from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.functions import col

def main():
    # Set up Spark session
    builder = (
        SparkSession.builder
        .appName("TestChangeDataFeed")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Define the Delta table path and register it
    delta_path = "/Users/udayag/spark-delta-taxi-pipeline/delta/yellow_taxi_2023_01_transformed"
    spark.sql(f"CREATE TABLE IF NOT EXISTS yellow_taxi_transformed USING delta LOCATION '{delta_path}'")

    # Load the Delta table
    delta_table = DeltaTable.forPath(spark, delta_path)

    # Simulate an update
    print("Simulating an update...")
    delta_table.update(
        condition="PULocationID = 161 AND trip_duration < 10 AND pickup_borough = 'Manhattan'",
        set={"trip_duration": col("trip_duration") + 1}
    )

    # Get the table history to find the latest version
    history = delta_table.history().orderBy("version").collect()
    latest_version = history[-1]['version']

    # Query the CDF for the versions affected by the update
    print("Querying Change Data Feed...")
    cdf_df = spark.sql(f"SELECT * FROM table_changes('yellow_taxi_transformed', {latest_version - 1}, {latest_version})")
    cdf_df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()