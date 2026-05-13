from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_RAW = PROJECT_ROOT / "data" / "raw"
LAKEHOUSE = PROJECT_ROOT / "lakehouse"
BRONZE = LAKEHOUSE / "bronze"
SILVER = LAKEHOUSE / "silver"
GOLD = LAKEHOUSE / "gold"
REPORTS = PROJECT_ROOT / "reports"
TRIPS_RAW = DATA_RAW / "yellow_tripdata_2024-01.parquet"
BRONZE_TRIPS = BRONZE / "trips"
SILVER_TRIPS = SILVER / "trips_clean"
REJECTED_TRIPS = SILVER / "trips_rejected"
GOLD_DAILY_REVENUE = GOLD / "daily_revenue"
GOLD_PAYMENT_TYPE = GOLD / "payment_type_summary"
GOLD_QUALITY_SUMMARY = GOLD / "quality_summary"