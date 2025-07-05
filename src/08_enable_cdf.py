from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    """
    Enable Change Data Feed (CDF) on the Delta table by registering it as a persistent table and altering its properties.
    """
    # Step 1: Set up Spark session with Delta Lake support
    builder = (
        SparkSession.builder
        .appName("EnableChangeDataFeed")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Step 2: Define the Delta table path
    delta_path = "/Users/udayag/spark-delta-taxi-pipeline/delta/yellow_taxi_2023_01_transformed"

    # Step 3: Register the Delta table as a persistent table in the default database
    print("Registering Delta table as a persistent table...")
    spark.sql(f"CREATE TABLE IF NOT EXISTS yellow_taxi_transformed USING delta LOCATION '{delta_path}'")

    # Step 4: Enable Change Data Feed
    print("Enabling Change Data Feed...")
    spark.sql("ALTER TABLE yellow_taxi_transformed SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    # Step 5: Verify the change
    print("Verifying table properties...")
    properties = spark.sql("SHOW TBLPROPERTIES yellow_taxi_transformed").collect()
    for row in properties:
        print(f"{row['key']}: {row['value']}")
    if any(row['key'] == 'delta.enableChangeDataFeed' and row['value'] == 'true' for row in properties):
        print("Success: Change Data Feed is enabled!")
    else:
        print("Warning: Change Data Feed may not have been enabled correctly.")

    # Step 6: Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()