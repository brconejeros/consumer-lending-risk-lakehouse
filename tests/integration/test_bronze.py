"""Bronze coverage against the real Azure Databricks workspace over
Databricks Connect - moved here from `tests/unit/test_bronze.py` (see
CLAUDE.md "OOP ingestion framework"). The project deliberately tests
against real infrastructure now that Databricks Connect access exists,
rather than a local pyspark+delta-spark session against synthetic data.

Config-derivation tests are pure Python string logic (no Spark involved)
and moved over unchanged. The Spark-backed tests use `application_test`
(smallest real table, 48,744 rows) to keep runtime/cost reasonable, and
read the real ADLS landing zone / write the real `bronze` schema.

There's no shared filesystem between this machine and the remote
serverless cluster, so the local unit tests' `tmp_path`/`landing_path_override`
trick can't point at a local directory. The idempotency/schema-change tests
instead write synthetic Parquet to a Unity Catalog Volume scratch path
(which the remote cluster can see) and target a scratch table name that is
clearly not one of the real 8 tables, so production Bronze data is never
touched.

Note this workspace has the public DBFS root disabled
(`DbfsDisabledException: Public DBFS root is disabled`, confirmed by
actually hitting it) - a Unity Catalog governance default, not a bug - so
`dbfs:/tmp/...` doesn't work as scratch storage here the way it would on an
older/non-UC workspace. A Unity Catalog Volume
(`consumer_lending_risk_lakehouse.bronze.bronze_test_scratch`, created
once, idempotently, by the `scratch_config` fixture) is the correct
UC-governed equivalent. Cleanup uses the Databricks SDK's Files API
(`WorkspaceClient().files`) recursively, since `delete_directory` refuses
non-empty directories and there's no recursive flag.
"""

import os
import uuid

import pytest

from src.lakehouse.bronze import BronzeIngestionJob, BronzeTableConfig

SCRATCH_VOLUME = "consumer_lending_risk_lakehouse.bronze.bronze_test_scratch"
SCRATCH_VOLUME_PATH = "/Volumes/consumer_lending_risk_lakehouse/bronze/bronze_test_scratch"


def test_config_derives_landing_path_and_target_table():
    config = BronzeTableConfig(table="bureau")

    assert config.landing_path == (
        "abfss://landing@streditorigination01.dfs.core.windows.net/bureau/"
    )
    assert config.target_table == "consumer_lending_risk_lakehouse.bronze.bureau"


def test_config_landing_path_override_bypasses_adls_url():
    config = BronzeTableConfig(table="bureau", landing_path_override="/tmp/landing/bureau")

    assert config.landing_path == "/tmp/landing/bureau"


def test_config_reflects_pos_cash_balance_source_casing():
    config = BronzeTableConfig(table="POS_CASH_balance")

    assert config.landing_path.endswith("/POS_CASH_balance/")
    assert config.target_table == "consumer_lending_risk_lakehouse.bronze.POS_CASH_balance"


def test_extract_reads_real_application_test_landing_zone(spark):
    config = BronzeTableConfig(table="application_test")
    job = BronzeIngestionJob(spark, config)

    result = job.extract()

    assert result.count() == 48744


def test_run_lands_real_application_test_in_bronze(spark):
    config = BronzeTableConfig(table="application_test")
    job = BronzeIngestionJob(spark, config)

    job.run()

    written = spark.table(config.target_table)
    assert written.count() == 48744


def _delete_volume_dir_recursive(files_client, directory_path: str) -> None:
    """`WorkspaceClient().files.delete_directory` refuses non-empty
    directories and offers no recursive flag, so walk and delete bottom-up.
    """
    for entry in list(files_client.list_directory_contents(directory_path)):
        if entry.is_directory:
            _delete_volume_dir_recursive(files_client, entry.path)
            files_client.delete_directory(entry.path)
        else:
            files_client.delete(entry.path)
    files_client.delete_directory(directory_path)


@pytest.fixture
def scratch_config(spark):
    """A BronzeTableConfig pointed at a scratch Unity Catalog Volume landing
    path and a scratch Unity Catalog table - never one of the real 8 tables
    - so the idempotency/schema-change tests below can't touch production
    Bronze data. Creates the scratch volume idempotently (a no-op after the
    first run) and cleans up this test's own subfolder and table afterward.
    """
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCRATCH_VOLUME}")

    scratch_name = f"bronze_test_scratch_{uuid.uuid4().hex[:8]}"
    volume_subdir = f"{SCRATCH_VOLUME_PATH}/{scratch_name}"
    config = BronzeTableConfig(
        table=scratch_name,
        landing_path_override=f"{volume_subdir}/",
    )

    yield config

    spark.sql(f"DROP TABLE IF EXISTS {config.target_table}")

    from databricks.sdk import WorkspaceClient

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    workspace_client = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    _delete_volume_dir_recursive(workspace_client.files, volume_subdir)


def test_rerun_overwrites_rather_than_appends(spark, scratch_config):
    job = BronzeIngestionJob(spark, scratch_config)

    spark.createDataFrame([(1,)], ["id"]).write.mode("overwrite").parquet(
        scratch_config.landing_path
    )
    job.run()
    assert spark.table(scratch_config.target_table).count() == 1

    spark.createDataFrame([(1,), (2,)], ["id"]).write.mode("overwrite").parquet(
        scratch_config.landing_path
    )
    job.run()
    assert spark.table(scratch_config.target_table).count() == 2


def test_rerun_with_changed_schema_overwrites_schema(spark, scratch_config):
    job = BronzeIngestionJob(spark, scratch_config)

    spark.createDataFrame([(1,)], ["id"]).write.mode("overwrite").parquet(
        scratch_config.landing_path
    )
    job.run()
    assert spark.table(scratch_config.target_table).columns == ["id"]

    spark.createDataFrame([(1, "x")], ["id", "value"]).write.mode("overwrite").parquet(
        scratch_config.landing_path
    )
    job.run()
    assert spark.table(scratch_config.target_table).columns == ["id", "value"]
