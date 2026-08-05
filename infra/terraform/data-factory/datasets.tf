# azurerm_data_factory_dataset_postgresql (the typed resource) pairs with the
# legacy V1 "PostgreSql" linked service type - it isn't clear it pairs
# correctly with the PostgreSqlV2 custom linked service in linked_services.tf
# (used specifically to dodge a confirmed SSL bug in the V1 typed resource,
# see linked_services.tf). Using azurerm_data_factory_custom_dataset instead
# to stay explicitly V2 end-to-end (linked service, dataset, and the Copy
# activity's source type in pipeline.tf all say V2) rather than risk a type
# mismatch between a V1-shaped dataset and a V2 linked service.
resource "azurerm_data_factory_custom_dataset" "postgres_source" {
  name            = "ds_postgres_table"
  data_factory_id = azurerm_data_factory.main.id
  type            = "PostgreSqlV2Table"

  linked_service {
    name = azurerm_data_factory_linked_custom_service.postgres.name
  }

  parameters = {
    tableName = ""
  }

  # TODO verify the exact property name (table vs schema+table) the
  # PostgreSqlV2Table dataset type expects against Microsoft's reference on
  # first apply. Quoted like load_csvs.py's own '"{table_name}"' quoting -
  # POS_CASH_balance and friends are mixed-case identifiers Postgres would
  # otherwise fold to lowercase.
  type_properties_json = jsonencode({
    table = {
      value = "\"@{dataset().tableName}\""
      type  = "Expression"
    }
  })
}

resource "azurerm_data_factory_dataset_parquet" "landing" {
  name                = "ds_parquet_landing"
  data_factory_id     = azurerm_data_factory.main.id
  linked_service_name = azurerm_data_factory_linked_service_data_lake_storage_gen2.landing.name

  # The provider sends an empty string if this is left unset, which the
  # Parquet sink rejects outright (fails to parse "" as a codec enum) rather
  # than treating it as "no compression" - must be an explicit valid value.
  compression_codec = "snappy"

  parameters = {
    tableName = ""
  }

  azure_blob_fs_location {
    file_system          = azurerm_storage_data_lake_gen2_filesystem.landing.name
    path                 = "@dataset().tableName"
    dynamic_path_enabled = true
    filename             = "part-0000.parquet"
  }
}
