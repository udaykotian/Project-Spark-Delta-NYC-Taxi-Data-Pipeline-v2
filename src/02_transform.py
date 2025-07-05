from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, unix_timestamp, when, current_timestamp, broadcast

def main():
    """
    Main function to transform Yellow Taxi data with a broadcast join and save it as a partitioned Delta table.
    """
    # Step 1: Set up Spark session with Delta Lake support
    builder = (
        SparkSession.builder
        .appName("TransformYellowTaxiDataWithJoins")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Step 2: Define file paths
    delta_path = "delta/yellow_taxi_2023_01"
    zone_path = "data/taxi_zone_lookup.csv"
    transformed_path = "delta/yellow_taxi_2023_01_transformed"

    # Step 3: Load raw data from Delta table
    df = spark.read.format("delta").load(delta_path)

    # Step 4: Load taxi zone lookup data from CSV
    zone_df = spark.read.csv(zone_path, header=True, inferSchema=True)

    # Step 5: Transform the data
    df_transformed = (
        df.withColumn("passenger_count", when(col("passenger_count").isNull(), 0).otherwise(col("passenger_count")))
        .withColumn(
            "trip_duration",
            (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
        )
        .filter((col("trip_distance") >= 0) & (col("trip_duration") > 0))
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    # Step 6: Perform a broadcast join with taxi zone data
    df_transformed = df_transformed.join(
        broadcast(zone_df),
        df_transformed["PULocationID"] == zone_df["LocationID"],
        "left"
    ).select(df_transformed["*"], zone_df["Borough"].alias("pickup_borough"))

    # Step 7: Save the transformed data as a partitioned Delta table
    df_transformed.write.format("delta") \
        .partitionBy("pickup_borough") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .save(transformed_path)

    # Step 8: Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()