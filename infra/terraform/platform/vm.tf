resource "azurerm_public_ip" "airbyte_vm" {
  name                = "airbyte-vm-pip"
  location            = var.location
  resource_group_name = data.azurerm_resource_group.ingestion.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "airbyte_vm" {
  name                = "airbyte-vm-nic"
  location            = var.location
  resource_group_name = data.azurerm_resource_group.ingestion.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.airbyte.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.airbyte_vm.id
  }
}

resource "azurerm_linux_virtual_machine" "airbyte" {
  name                = "airbyte-vm"
  resource_group_name = data.azurerm_resource_group.ingestion.name
  location            = var.location
  size                = var.vm_size
  admin_username      = var.vm_admin_username

  network_interface_ids           = [azurerm_network_interface.airbyte_vm.id]
  disable_password_authentication = true

  admin_ssh_key {
    username   = var.vm_admin_username
    public_key = file(pathexpand(var.ssh_public_key_path))
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = 64
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = var.vm_image_sku
    version   = "latest"
  }

  custom_data = base64encode(templatefile("${path.module}/cloud-init.yaml", {
    admin_username = var.vm_admin_username
  }))
}
