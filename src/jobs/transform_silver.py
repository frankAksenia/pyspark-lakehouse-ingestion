from pyspark.sql import functions as F

from src.utils.paths import BRONZE_TRIPS, SILVER_TRIPS, REJECTED_TRIPS
from src.utils.spark import get_spark


def clean_trips(df):
    cleaned = df.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("double").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("double").alias("rate_code_id"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("payment_type").cast("long").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("ingest_timestamp"),
        F.col("source_file"),
    )

    cleaned = (
        cleaned.withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("trip_duration_minutes", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts"))/ 60,)
        .withColumn("avg_speed_mph", F.when(F.col("trip_duration_minutes") > 0, F.col("trip_distance") / (F.col("trip_duration_minutes") / 60),),)
        .withColumn("fare_per_mile", F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance"),),
        )
    )

    return cleaned


def add_rejection_reason(df):
    return df.withColumn(
        "rejection_reason",
        F.concat_ws("; ", F.when(F.col("pickup_ts").isNull(),F.lit("pickup timestamp is null")),
            F.when(F.col("dropoff_ts").isNull(), F.lit("dropoff timestamp is null")),
            F.when(F.col("dropoff_ts") <= F.col("pickup_ts"), F.lit("dropoff before or equal to pickup"),),
            F.when(F.col("trip_duration_minutes").isNull(), F.lit("trip duration is null"),),
            F.when(F.col("trip_duration_minutes") < 1, F.lit("trip duration below 1 minute"),),F.when(F.col("trip_duration_minutes") > 240, F.lit("trip duration above 240 minutes"),),
            F.when(F.col("trip_distance").isNull(), F.lit("trip distance is null")),
            F.when(F.col("trip_distance") <= 0, F.lit("trip distance is not positive"),),
            F.when(F.col("trip_distance") > 100, F.lit("trip distance above 100 miles"),),
            F.when(F.col("passenger_count").isNull(), F.lit("passenger count is null"),),
            F.when(~F.col("passenger_count").between(1, 6),F.lit("passenger count outside 1-6"),),
            F.when(F.col("fare_amount").isNull(), F.lit("fare amount is null")),
            F.when(F.col("fare_amount") < 0, F.lit("fare amount is negative")),
            F.when(F.col("total_amount").isNull(), F.lit("total amount is null")),
            F.when(F.col("total_amount") <= 0, F.lit("total amount is not positive"),),
            F.when(F.col("avg_speed_mph") > 100, F.lit("average speed above 100 mph"),),
            F.when(F.col("pickup_location_id").isNull(), F.lit("pickup location id is null"),),
            F.when(F.col("dropoff_location_id").isNull(), F.lit("dropoff location id is null"),
            ),
        ),
    )

def split_valid_and_rejected(df):
    checked = add_rejection_reason(df)

    valid = checked.filter(F.col("rejection_reason") == "").drop("rejection_reason")
    rejected = checked.filter(F.col("rejection_reason") != "")

    return valid, rejected


def main():
    spark = get_spark("transform_silver")

    bronze = spark.read.format("delta").load(str(BRONZE_TRIPS))

    cleaned = clean_trips(bronze)
    valid, rejected = split_valid_and_rejected(cleaned)

    valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(SILVER_TRIPS))

    rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(REJECTED_TRIPS))

    print(f"Silver valid rows: {valid.count()}")
    print(f"Rejected rows: {rejected.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
