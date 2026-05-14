from src.utils.quality_checks import check_not_null, check_positive, check_range


def test_quality_checks_for_failures(spark):

      rows = [(1, 10.0), (None, -5.0), (8, 0.0)]
      columns = ["passenger_count", "total_amount"]

      df = spark.createDataFrame(rows, columns)

      null_result = check_not_null(df, "passenger_count")    
      positive_result = check_positive(df, "total_amount")    
      range_result = check_range(df, "passenger_count", 1, 6)

      assert null_result.status == "FAIL"
      assert positive_result.failed_count == 2
      assert range_result.status == "FAIL"

    