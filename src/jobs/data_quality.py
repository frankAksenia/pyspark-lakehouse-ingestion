import json

from datetime import datetime, timezone

from src.utils.paths import SILVER_TRIPS, REJECTED_TRIPS, REPORTS

from src.utils.quality_checks import run_silver_trip_quality_checks, results_to_dicts
from src.utils.spark import get_spark


def main():
    spark = get_spark("data_quality_checks")

    silver_df = spark.read.format("delta").load(str(SILVER_TRIPS))
    rejected_df = spark.read.format("delta").load(str(REJECTED_TRIPS))

    check_results = run_silver_trip_quality_checks(silver_df)

    total_checks = len(check_results)
    failed_checks = [
        result for result in check_results if result.status == "FAIL"]

    report = {
        "report_generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": "nyc_yellow_taxi_trips",
        "layer": "silver",
        "summary": {
            "silver_rows": silver_df.count(),
            "rejected_rows": rejected_df.count(),
            "total_checks": total_checks,
            "failed_checks": len(failed_checks),
            "quality_status": "PASS" if len(failed_checks) == 0 else "FAIL",
        },
        "checks": results_to_dicts(check_results),
    }

    REPORTS.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS / "silver_data_quality_report.json"

    with open(report_path, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    print(json.dumps(report["summary"], indent=2))

    spark.stop()


if __name__ == "__main__":
    main()
   