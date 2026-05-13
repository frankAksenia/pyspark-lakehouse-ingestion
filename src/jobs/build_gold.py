from pyspark.sql import functions as F

from src.utils.paths import GOLD_DAILY_REVENUE, GOLD_HOURLY_DEMAND, GOLD_REVENUE_EFFICIENCY, SILVER_TRIPS
from src.utils.spark import get_spark


def main():
    spark = get_spark("build_gold")
    trips = spark.read.format("delta").load(str(SILVER_TRIPS))


    # Aggregates trip activity and revenue at daily level
    # Can be used by business users to monitor daily demand and revenue trends
    daily_revenue = (
        trips.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance")
        )
        .orderBy("pickup_date")
    )

    # Aggregates demand and operational performance by pickup date and pickup hour
    # Can be used for operational planning and identifying daily demand patterns
    hourly_demand = (
        trips.groupBy("pickup_date", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph") 
        )
        .orderBy("pickup_date", "pickup_hour")
    )

    # Calculates revenue efficiency metrics at daily level.
    # Can be used for operational performance monitoring.
    revenue_efficiency = (
        trips.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.sum("trip_distance"), 2).alias("total_miles"),
            F.round(F.sum("trip_duration_minutes"), 2).alias("total_minutes"),
        )
        .withColumn(
            "revenue_per_mile",
            F.round(F.col("total_revenue") / F.col("total_miles"), 2)
        )
        .withColumn(
            "revenue_per_hour",
            F.round(F.col("total_revenue") / (F.col("total_minutes") / 60), 2)
        )
        .orderBy("pickup_date")
    )

    daily_revenue.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(GOLD_DAILY_REVENUE))


    hourly_demand.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(GOLD_HOURLY_DEMAND))

    revenue_efficiency.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(GOLD_REVENUE_EFFICIENCY))


    print("Daily revenue sample:")
    daily_revenue.show(10, truncate=False)

    print("Hourly demand sample:")
    hourly_demand.show(10, truncate=False)

    print("Revenue efficiency sample:")
    revenue_efficiency.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
        main()