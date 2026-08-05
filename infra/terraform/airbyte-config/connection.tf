locals {
  # Xmin-based incremental_append (see Stack in CLAUDE.md) - no cursor_field
  # needed, Xmin is a Postgres system column the source connector reads
  # automatically. incremental (not full_refresh) so a later sync only pulls
  # rows changed since the last one, once "simulate ongoing originations" (a
  # documented future enhancement) starts writing to Postgres between syncs.
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

resource "airbyte_connection" "postgres_to_databricks" {
  name           = "Credit Origination -> Bronze"
  source_id      = airbyte_source.postgres.source_id
  destination_id = airbyte_destination.databricks.destination_id
  status         = "active"

  configurations = {
    streams = [
      for table in local.bronze_tables : {
        name      = table
        sync_mode = "incremental_append"
      }
    ]
  }

  # Manual trigger, not a cron schedule - Postgres/the VM are stopped between
  # work sessions (see infra/terraform/platform/toggle.sh), so a schedule
  # would just fail when nothing's running.
  schedule = {
    schedule_type = "manual"
  }
}
