output "postgres_fqdn" {
  description = "Fully qualified domain name of the Postgres Flexible Server"
  value       = azurerm_postgresql_flexible_server.origination.fqdn
}
