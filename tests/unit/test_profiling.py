from src.lakehouse.profiling import fk_orphan_count, null_rate


def test_null_rate_reports_fraction_of_nulls_per_column(spark):
    df = spark.createDataFrame([(1, None), (2, 200), (None, 300), (4, None)], ["a", "b"])

    result = null_rate(df, ["a", "b"]).collect()[0]

    assert result["a"] == 0.25
    assert result["b"] == 0.5


def test_fk_orphan_count_counts_unmatched_rows(spark):
    child = spark.createDataFrame([(1,), (2,), (99,)], ["id"])
    parent = spark.createDataFrame([(1,), (2,)], ["id"])

    assert fk_orphan_count(child, "id", parent, "id") == 1


def test_fk_orphan_count_is_zero_when_all_rows_match(spark):
    child = spark.createDataFrame([(1,), (2,)], ["id"])
    parent = spark.createDataFrame([(1,), (2,), (3,)], ["id"])

    assert fk_orphan_count(child, "id", parent, "id") == 0
