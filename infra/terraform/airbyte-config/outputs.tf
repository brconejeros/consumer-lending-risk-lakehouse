output "source_id" {
  description = "Airbyte's Postgres source connector ID"
  value       = airbyte_source.postgres.source_id
}

output "destination_id" {
  description = "Airbyte's Databricks destination connector ID"
  value       = airbyte_destination.databricks.destination_id
}

output "connection_id" {
  description = "The Postgres -> Databricks connection ID"
  value       = airbyte_connection.postgres_to_databricks.connection_id
}
