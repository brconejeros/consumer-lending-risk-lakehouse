def test_serverless_session_connects(spark):
    result = spark.sql("SELECT 1 AS one").collect()

    assert result[0]["one"] == 1
