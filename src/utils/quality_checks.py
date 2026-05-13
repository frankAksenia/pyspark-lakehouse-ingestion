from dataclasses import dataclass
from pyspark.sql import DataFrame, functions as F
from typing import Any
from dataclasses import asdict

@dataclass
class QualityCheckResult:
    check_name: str
    status: str
    failed_count: int
    total_count: int
    failure_rate: float

def _build_result(check_name: str, total_count: int, failed_count: int,) -> QualityCheckResult:
    failure_rate = failed_count / total_count if total_count > 0 else 0.0
    status = "PASS" if failed_count == 0 else "FAIL"

    return QualityCheckResult(
        check_name=check_name,
        status=status,
        failed_count=failed_count,
        total_count=total_count,
        failure_rate=failure_rate,
    )


def results_to_dicts(results: list[QualityCheckResult]) -> list[dict[str, Any]]:
    return [asdict(result) for result in results]

def check_not_null(df: DataFrame, column: str) -> QualityCheckResult:
    total_count = df.count()
    failed_count = df.filter(F.col(column).isNull()).count()
    
    return _build_result(f"Non-null check for {column}", total_count, failed_count)   

def check_positive(df: DataFrame, column: str) -> QualityCheckResult:
    total_count = df.count()
    failed_count = df.filter(F.col(column) <= 0).count()

    return _build_result(f"Positive value check for {column}", total_count, failed_count)

def check_range(df: DataFrame, column: str, min_value: float, max_value: float) -> QualityCheckResult:
    total_count = df.count()
    failed_count = df.filter((F.col(column) < min_value) | (F.col(column) > max_value)).count()

    return _build_result(f"Range check for {column} ({min_value} to {max_value})", total_count, failed_count)


def check_max_value(df: DataFrame, column: str, max_value: float) -> QualityCheckResult:
    total_count = df.count()
    failed_count = df.filter(F.col(column) > max_value).count()

    return _build_result(f"Maximum value check for {column} <= {max_value}", total_count, failed_count)


def check_timestamp_order(df: DataFrame, start_column: str, end_column: str) -> QualityCheckResult:
    total_count = df.count()
    failed_count = df.filter(F.col(end_column) <= F.col(start_column)).count()

    return _build_result(f"Timestamp order check: {end_column} > {start_column}", total_count, failed_count,
    )


def check_duplicates(df: DataFrame, subset: list) -> QualityCheckResult:
    total_count = df.count()
    distinct = df.dropDuplicates(subset).count()
    failed_count = total_count - distinct

    return _build_result("Duplicate check for columns {subset}", total_count, failed_count)            


def run_silver_trip_quality_checks(df: DataFrame) -> list[QualityCheckResult]:
    """
    Runs all quality checks that should be true for silver.trips_clean.

    These checks mirror the cleaning and rejection rules from transform_silver.py.
    If trips_clean was built correctly, all checks should PASS.
    """

    checks = [
        check_not_null(df, "pickup_ts"),
        check_not_null(df, "dropoff_ts"),
        check_timestamp_order(df, "pickup_ts", "dropoff_ts"),

        check_not_null(df, "trip_duration_minutes"),
        check_range(df, "trip_duration_minutes", 1, 240),

        check_not_null(df, "trip_distance"),
        check_range(df, "trip_distance", 0.000001, 100),

        check_not_null(df, "passenger_count"),
        check_range(df, "passenger_count", 1, 6),

        check_not_null(df, "fare_amount"),
        check_positive(df, "fare_amount"),

        check_not_null(df, "total_amount"),
        check_positive(df, "total_amount"),

        check_max_value(df, "avg_speed_mph", 100),

        check_not_null(df, "pickup_location_id"),
        check_not_null(df, "dropoff_location_id"),
    ]

    return checks
