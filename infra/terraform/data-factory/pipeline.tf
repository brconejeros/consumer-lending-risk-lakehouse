locals {
  # Same 8 tables as infra/postgres/load_csvs.py - keep these lists in sync
  # by eyeballing, no shared-constants mechanism needed at this scale.
  bronze_tables = [
    "application_train",
    "application_test",
    "bureau",
    "bureau_balance",
    "previous_application",
    "POS_CASH_balance",
    "installments_payments",
    "credit_card_balance",
  ]
}

resource "azurerm_data_factory_pipeline" "bronze_landing" {
  name            = "copy_postgres_to_landing"
  data_factory_id = azurerm_data_factory.main.id

  # One ForEach wrapping one parameterized Copy activity. ADF's activity
  # graph isn't fully modeled as typed HCL, so this is opaque JSON to
  # Terraform - real validation only happens against the live ADF REST API
  # on first apply.
  #
  # isSequential = false with a batchCount cap (not unlimited parallelism):
  # originally sequential, reacting to the old Airbyte setup's
  # concurrency-driven OOM failures on self-hosted sync pods - a
  # resource-constrained-compute failure mode that doesn't transfer to
  # ADF's Copy Activity, which is fully managed and DIU-scaled, not running
  # on a memory-capped VM. Capping at 4 (rather than ADF's own 50-way
  # default under isSequential=false) keeps most of the speed-up while
  # staying the "boring, debuggable" choice this project has favored since
  # that Airbyte experience.
  activities_json = jsonencode([
    {
      name = "ForEachBronzeTable"
      type = "ForEach"
      typeProperties = {
        isSequential = false
        batchCount   = 4
        items = {
          value = "@json('${jsonencode(local.bronze_tables)}')"
          type  = "Expression"
        }
        activities = [
          {
            name = "CopyTable"
            type = "Copy"
            inputs = [{
              referenceName = azurerm_data_factory_custom_dataset.postgres_source.name
              type          = "DatasetReference"
              parameters    = { tableName = "@item()" }
            }]
            outputs = [{
              referenceName = azurerm_data_factory_dataset_parquet.landing.name
              type          = "DatasetReference"
              parameters    = { tableName = "@item()" }
            }]
            typeProperties = {
              source = {
                type = "PostgreSqlV2Source"
                query = {
                  value = "SELECT * FROM \"@{item()}\""
                  type  = "Expression"
                }
              }
              sink = {
                type          = "ParquetSink"
                storeSettings = { type = "AzureBlobFSWriteSettings" }
              }
            }
          }
        ]
      }
    }
  ])
}
