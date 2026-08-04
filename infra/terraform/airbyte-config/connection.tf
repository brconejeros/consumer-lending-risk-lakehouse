resource "airbyte_connection" "postgres_to_databricks" {
  name           = "Credit Origination -> Bronze"
  source_id      = airbyte_source.postgres.source_id
  destination_id = airbyte_destination.databricks.destination_id
  status         = "active"

  # Manual trigger, not a cron schedule - Postgres/the VM are stopped between
  # work sessions (see infra/terraform/platform/toggle.sh), so a schedule
  # would just fail when nothing's running.
  schedule = {
    schedule_type = "manual"
  }
}
