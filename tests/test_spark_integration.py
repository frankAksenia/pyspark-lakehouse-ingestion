from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

from src.utils.spark import get_spark
from src.jobs.transform_silver import clean_trips, add_rejection_reason


def test_silver_transformation_splits_valid_and_invalid_trips():
    spark = get_spark("test_silver_transformation")

    rows = [
        Row(
            VendorID=1,
            tpep_pickup_datetime=datetime(2024, 1, 1, 10, 0, 0),
            tpep_dropoff_datetime=datetime(2024, 1, 1, 10, 15, 0),
            passenger_count=1.0,
            trip_distance=3.2,
            RatecodeID=1.0,
            PULocationID=100,
            DOLocationID=200,
            payment_type=1,
            fare_amount=18.0,
            tip_amount=3.0,
            total_amount=24.0,
            ingest_timestamp=datetime(2024, 1, 1, 12, 0, 0),
            source_file="test_valid.parquet",
        ),
        Row(
            VendorID=2,
            tpep_pickup_datetime=datetime(2024, 1, 1, 11, 0, 0),
            tpep_dropoff_datetime=datetime(2024, 1, 1, 11, 10, 0),
            passenger_count=0.0,
            trip_distance=2.0,
            RatecodeID=1.0,
            PULocationID=101,
            DOLocationID=201,
            payment_type=2,
            fare_amount=10.0,
            tip_amount=0.0,
            total_amount=12.0,
            ingest_timestamp=datetime(2024, 1, 1, 12, 0, 0),
            source_file="test_invalid.parquet",
        ),
    ]

    raw_df = spark.createDataFrame(rows)

    checked_df = add_rejection_reason(clean_trips(raw_df))

    valid_df = checked_df.filter(F.col("rejection_reason") == "")
    rejected_df = checked_df.filter(F.col("rejection_reason") != "")

    assert valid_df.count() == 1
    assert rejected_df.count() == 1

    rejected_reason = rejected_df.select("rejection_reason").collect()[0][0]
    assert "passenger count outside 1-6" in rejected_reason

    valid_row = valid_df.collect()[0]

    assert valid_row.vendor_id == 1
    assert valid_row.pickup_date is not None
    assert valid_row.pickup_hour == 10
    assert round(valid_row.trip_duration_minutes, 2) == 15.0
    assert round(valid_row.avg_speed_mph, 2) == 12.8
    assert round(valid_row.fare_per_mile, 2) == 5.62

    spark.stop()
