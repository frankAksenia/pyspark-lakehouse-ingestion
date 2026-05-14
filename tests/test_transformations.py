from datetime import datetime

from src.jobs.transform_silver import add_rejection_reason, clean_trips


def test_clean_trips_derives_duration_and_standard_columns(spark):

    rows = [(1, datetime(2024, 1, 1, 10, 0), datetime(2024, 1, 1, 10, 15), 1.0, 2.5,
             1.0, 100, 200, 1, 10.0, 2.0, 12.0, datetime(2024, 1, 1, 10, 20), "test")]
    columns = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "tip_amount", "total_amount", "ingest_timestamp", "source_file"]

    df = spark.createDataFrame(rows, columns)
    result = clean_trips(df).collect()[0]

    assert result.trip_duration_minutes == 15.0
    assert result.pickup_location_id == 100


    def test_add_rejection_reason_flags_invalid_trip(spark):

        rows= [(1, datetime(2024, 1, 1, 10, 0), datetime(2024, 1, 1, 9, 0), 0.0, -1.0, 10.0)]
        columns= ["vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance", "total_amount"]

        df= spark.createDataFrame(rows, columns)

        result = add_rejection_reason(df).collect()[0]

        assert "dropoff before or equal to pickup" in result.rejection_reason
        assert "trip distance is not positive" in result.rejection_reason
                                  