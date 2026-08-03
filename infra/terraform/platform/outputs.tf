output "postgres_fqdn" {
  description = "Fully qualified domain name of the Postgres Flexible Server"
  value       = azurerm_postgresql_flexible_server.origination.fqdn
}

output "airbyte_vm_public_ip" {
  description = "Public IP of the Airbyte VM (SSH + web UI on port 8000)"
  value       = azurerm_public_ip.airbyte_vm.ip_address
}
