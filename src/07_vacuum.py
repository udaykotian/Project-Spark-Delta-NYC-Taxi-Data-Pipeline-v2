from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

def main():
    """
    Clean up old files in the Delta table using VACUUM to maintain storage efficiency.
    """
    # Step 1: Set up Spark session with Delta Lake support
    builder = (
        SparkSession.builder
        .appName("VacuumDeltaTable")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Step 2: Load the Delta table
    delta_path = "delta/yellow_taxi_2023_01_transformed"
    delta_table = DeltaTable.forPath(spark, delta_path)

    # Step 3: Vacuum old files (retain data for 7 days)
    print("Running VACUUM to clean up old files...")
    delta_table.vacuum(retentionHours=168)  # Retains files for 7 days (168 hours)

    # Step 4: Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()