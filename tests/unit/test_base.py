import pytest

from src.lakehouse.base import LakehouseLayerJob


def test_lakehouse_layer_job_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        LakehouseLayerJob(None)


def test_run_calls_hooks_in_order(spark):
    calls = []

    class RecordingJob(LakehouseLayerJob):
        layer = "test"

        def extract(self):
            calls.append("extract")
            return spark.createDataFrame([(1,)], ["id"])

        def transform(self, df):
            calls.append("transform")
            return df

        def validate(self, df):
            calls.append("validate")

        def load(self, df):
            calls.append("load")

    RecordingJob(spark).run()

    assert calls == ["extract", "transform", "validate", "load"]


def test_transform_and_validate_default_to_no_ops(spark):
    class MinimalJob(LakehouseLayerJob):
        layer = "test"
        loaded = None

        def extract(self):
            return spark.createDataFrame([(1,), (2,)], ["id"])

        def load(self, df):
            MinimalJob.loaded = df

    result = MinimalJob(spark).run()

    assert result is MinimalJob.loaded
    assert sorted(r["id"] for r in result.collect()) == [1, 2]
