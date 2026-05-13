from pyspark.sql import functions as F
from src.utils.paths import BRONZE_TRIPS, TRIPS_RAW
from src.utils.spark import get_spark


def main():
    spark = get_spark("ingest_bronze")

    df = (
        spark.read.parquet(str(TRIPS_RAW))
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")  
        .option("overwriteSchema", "true")
        .save(str(BRONZE_TRIPS))
    )

    print(f"Ingested {df.count()} records to {BRONZE_TRIPS}")

    spark.stop()


if __name__ == "__main__":
    main()
