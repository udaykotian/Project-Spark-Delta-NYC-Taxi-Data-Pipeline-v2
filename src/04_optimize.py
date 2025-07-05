from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

def main():
    """
    Optimize the Delta table by compacting files and clustering data with ZORDER BY.
    """
    # Step 1: Set up Spark session with Delta Lake support
    builder = (
        SparkSession.builder
        .appName("OptimizeDeltaTable")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Step 2: Load the Delta table
    delta_table = DeltaTable.forPath(spark, "delta/yellow_taxi_2023_01_transformed")

    # Step 3: Compact files with OPTIMIZE
    print("Running OPTIMIZE compaction...")
    delta_table.optimize().executeCompaction()

    # Step 4: Cluster data with ZORDER BY
    print("Running ZORDER BY on PULocationID...")
    delta_table.optimize().executeZOrderBy("PULocationID")

    # Step 5: Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()