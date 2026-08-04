# Created out-of-band via az cli during bootstrap (holds the tfstate storage
# account too) - referenced here, not managed, so `terraform destroy` never
# touches it.
data "azurerm_resource_group" "ingestion" {
  name = var.resource_group_name
}

resource "azurerm_virtual_network" "ingestion" {
  name                = "ingestion-vnet"
  address_space       = ["10.20.0.0/16"]
  location            = var.vm_location
  resource_group_name = data.azurerm_resource_group.ingestion.name
}

resource "azurerm_subnet" "airbyte" {
  name                 = "airbyte-subnet"
  resource_group_name  = data.azurerm_resource_group.ingestion.name
  virtual_network_name = azurerm_virtual_network.ingestion.name
  address_prefixes     = ["10.20.1.0/24"]
}

resource "azurerm_network_security_group" "airbyte_vm" {
  name                = "airbyte-vm-nsg"
  location            = var.vm_location
  resource_group_name = data.azurerm_resource_group.ingestion.name

  security_rule {
    name                       = "AllowSSHFromAdminIP"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "${var.admin_ip_address}/32"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "AllowAirbyteUIFromAdminIP"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8000"
    source_address_prefix      = "${var.admin_ip_address}/32"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "airbyte" {
  subnet_id                 = azurerm_subnet.airbyte.id
  network_security_group_id = azurerm_network_security_group.airbyte_vm.id
}
