from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)

        .master("local[2]")

        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.blockManager.port", "10025")
        .config("spark.driver.port", "10026")

        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")

        .config("spark.network.timeout", "300s")
        .config("spark.executor.heartbeatInterval", "30s")

        .config("spark.ui.showConsoleProgress", "false")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
