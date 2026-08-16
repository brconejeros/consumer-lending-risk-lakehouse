"""Read-only proof that SilverTransformJob works against a real Bronze
Unity Catalog table over Databricks Connect. No `load()` call here - stays
read-only against production data until real tables get wired up with
their own `SilverTableConfig` (see CLAUDE.md "Status").
"""

from src.lakehouse.silver import SilverTableConfig, SilverTransformJob


def test_transform_renames_real_bronze_application_train_columns(spark):
    job = SilverTransformJob(spark, SilverTableConfig(table="application_train"))

    raw = job.extract()
    result = job.transform(raw)

    assert "CurrId" in result.columns
    assert "IncomeTotalAmt" in result.columns
    assert "SK_ID_CURR" not in result.columns
