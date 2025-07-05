from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

def main():
    # Set up Spark session with Delta Lake support
    builder = (
        SparkSession.builder
        .appName("ExploreTransformedDeltaTable")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Load the transformed Delta table
    transformed_path = "delta/yellow_taxi_2023_01_transformed"
    delta_table = DeltaTable.forPath(spark, transformed_path)

    # Explore the data and history
    print(f"Row count: {delta_table.toDF().count()}")
    print("Table Schema:")
    delta_table.toDF().printSchema()
    print("Sample Data:")
    delta_table.toDF().select("PULocationID", "pickup_borough", "trip_duration").show(5)
    print("Operation History:")
    delta_table.history().show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()