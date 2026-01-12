terraform {
  required_providers {
    proxmox = {
      source  = "bpg/proxmox"
      version = "0.78.2"
    }
  }
}

# Configurazione del provider Proxmox
provider "proxmox" {
  endpoint = var.proxmox_api_url
  username = var.proxmox_user
  password = var.proxmox_password
  insecure = true

  ssh {
    agent    = true
    username = var.proxmox_host_user
  }
}

# Creazione delle VM con full clone del template usando BPG provider
resource "proxmox_virtual_environment_vm" "ubuntu-vm" {
  count       = var.vm_count
  name        = "${var.vm_name_prefix}-${count.index + 1}"
  description = "VM ${count.index + 1} creata con Terraform"
  node_name   = var.target_node

  # Optional deployment delay using local-exec provisioner
  provisioner "local-exec" {
    command = count.index == 0 || var.deployment_delay == 0 ? "echo 'Deploying ${var.vm_name_prefix}-${count.index + 1}...'" : "echo 'Waiting ${var.deployment_delay}s before deploying ${var.vm_name_prefix}-${count.index + 1}...' && sleep ${var.deployment_delay}"
  }

  # Full clone del template (mantiene tutte le configurazioni)
  clone {
    vm_id = var.template_id
    full  = true
  }

  # Network configuration - ensure each VM gets a unique MAC
  network_device {
    bridge      = "vmbr1"
    model       = "virtio"
    mac_address = format("02:00:00:00:%02x:%02x", count.index + 1, count.index + 100)
  }

  # Cloud-init configuration
  initialization {
    ip_config {
      ipv4 {
        address = "dhcp"
      }
    }

    user_account {
      username = var.ci_user
      password = var.ci_password
      keys     = [trimspace(file(var.ssh_key_path))]
    }

  }

  # Start VM after creation
  started = true
}

resource "null_resource" "wait_for_vm" {
  count      = var.vm_count
  depends_on = [proxmox_virtual_environment_vm.ubuntu-vm]

  # Create dependency chain: each VM waits for the previous one to complete
  # Use a different approach to avoid cycles
  triggers = {
    vm_id = proxmox_virtual_environment_vm.ubuntu-vm[count.index].vm_id
    # Simple timestamp-based trigger to ensure sequential execution
    timestamp = timestamp()
    # Add index to make each resource unique
    index = count.index
  }

  provisioner "local-exec" {
    # Add a delay for sequential execution (except for first VM)
    command = count.index == 0 ? "./wait_for_vm.sh ${proxmox_virtual_environment_vm.ubuntu-vm[count.index].vm_id} ${var.proxmox_host_user} ${var.proxmox_host}" : "sleep $((${count.index} * 0)) && ./wait_for_vm.sh ${proxmox_virtual_environment_vm.ubuntu-vm[count.index].vm_id} ${var.proxmox_host_user} ${var.proxmox_host}"
  }
}

# Read the IP from the file created by the script for each VM
data "local_file" "vm_ip" {
  count      = var.vm_count
  depends_on = [null_resource.wait_for_vm]
  filename   = "/tmp/vm_${proxmox_virtual_environment_vm.ubuntu-vm[count.index].vm_id}_ip.txt"
}

# Output per gli IP assegnati dal DHCP
output "vm_ips" {
  value = {
    for i in range(var.vm_count) :
    proxmox_virtual_environment_vm.ubuntu-vm[i].name => chomp(data.local_file.vm_ip[i].content)
  }
  depends_on = [data.local_file.vm_ip]
}

# Generate variables for the J2 template
locals {
  # Determine role for each VM
  vm_roles_resolved = {
    for i in range(var.vm_count) :
    proxmox_virtual_environment_vm.ubuntu-vm[i].name => lookup(
      var.vm_roles,
      proxmox_virtual_environment_vm.ubuntu-vm[i].name,
      var.default_vm_role
    )
  }

  # Create port mappings for each VM service
  port_mappings = merge(
    # SSH port mappings
    {
      for i in range(var.vm_count) :
      "${proxmox_virtual_environment_vm.ubuntu-vm[i].name}_ssh" => 2200 + i
    },
    # K3s port mappings - only for VMs with k3s role
    {
      for i in range(var.vm_count) :
      "${proxmox_virtual_environment_vm.ubuntu-vm[i].name}_k3s" => 6443 + i
      if local.vm_roles_resolved[proxmox_virtual_environment_vm.ubuntu-vm[i].name] == "k3s"
    },
    # Docker port mappings - only for VMs with docker role
    {
      for i in range(var.vm_count) :
      "${proxmox_virtual_environment_vm.ubuntu-vm[i].name}_docker" => 2375 + i
      if local.vm_roles_resolved[proxmox_virtual_environment_vm.ubuntu-vm[i].name] == "docker"
    }
  )

  # Template variables
  template_vars = {
    # Proxmox host information
    proxmox_host     = var.proxmox_host
    proxmox_user     = var.proxmox_host_user
    proxmox_ssh_key  = var.ssh_key_path
    target_interface = "vmbr1"
    source_interface = "vmbr0"

    # VM information for port mappings
    vm_services = flatten([
      for i in range(var.vm_count) : concat(
        # SSH service for all VMs
        [{
          name    = "${proxmox_virtual_environment_vm.ubuntu-vm[i].name}_ssh"
          vm_id   = proxmox_virtual_environment_vm.ubuntu-vm[i].vm_id
          vm_name = proxmox_virtual_environment_vm.ubuntu-vm[i].name
          vm_ip   = chomp(data.local_file.vm_ip[i].content)
          vm_port = 22
          service = "SSH"
          vm_user = var.ci_user
          vm_role = local.vm_roles_resolved[proxmox_virtual_environment_vm.ubuntu-vm[i].name]
        }],
        # K3s service for VMs with k3s role
        local.vm_roles_resolved[proxmox_virtual_environment_vm.ubuntu-vm[i].name] == "k3s" ? [{
          name    = "${proxmox_virtual_environment_vm.ubuntu-vm[i].name}_k3s"
          vm_id   = proxmox_virtual_environment_vm.ubuntu-vm[i].vm_id
          vm_name = proxmox_virtual_environment_vm.ubuntu-vm[i].name
          vm_ip   = chomp(data.local_file.vm_ip[i].content)
          vm_port = 6443
          service = "k3s"
          vm_user = var.ci_user
          vm_role = "k3s"
        }] : [],
        # Docker service for VMs with docker role
        local.vm_roles_resolved[proxmox_virtual_environment_vm.ubuntu-vm[i].name] == "docker" ? [{
          name    = "${proxmox_virtual_environment_vm.ubuntu-vm[i].name}_docker"
          vm_id   = proxmox_virtual_environment_vm.ubuntu-vm[i].vm_id
          vm_name = proxmox_virtual_environment_vm.ubuntu-vm[i].name
          vm_ip   = chomp(data.local_file.vm_ip[i].content)
          vm_port = 2375
          service = "docker"
          vm_user = var.ci_user
          vm_role = "docker"
        }] : []
      )
    ])
  }
}

# Generate the inventory file using template_file
resource "local_file" "inventory_nat_rules" {
  depends_on = [data.local_file.vm_ip]

  content = templatefile("${path.module}/../templates/inventory-nat-rules.ini.tpl", {
    proxmox_host     = var.proxmox_host
    proxmox_user     = var.proxmox_host_user
    proxmox_ssh_key  = "~/.ssh/id_rsa"
    target_interface = "vmbr1"
    source_interface = "vmbr0"
    vm_services      = local.template_vars.vm_services
  })

  filename = "${path.module}/../inventories/inventory-nat-rules.ini"

  # Ensure directory exists
  provisioner "local-exec" {
    command = "mkdir -p ${path.module}/../inventories"
  }
}

# Output per tutti gli ID delle VM
output "vm_ids" {
  value = {
    for i in range(var.vm_count) :
    proxmox_virtual_environment_vm.ubuntu-vm[i].name => proxmox_virtual_environment_vm.ubuntu-vm[i].vm_id
  }
}

# Output per tutti i MAC address
output "vm_macs" {
  value = {
    for i in range(var.vm_count) :
    proxmox_virtual_environment_vm.ubuntu-vm[i].name => format("02:00:00:00:%02x:%02x", i + 1, i + 100)
  }
  description = "Generated unique MAC addresses for each VM"
}

# Output per tutti i nomi delle VM
output "vm_names" {
  value = [for vm in proxmox_virtual_environment_vm.ubuntu-vm : vm.name]
}

# Output per tutti i nodi
output "vm_nodes" {
  value = {
    for i in range(var.vm_count) :
    proxmox_virtual_environment_vm.ubuntu-vm[i].name => proxmox_virtual_environment_vm.ubuntu-vm[i].node_name
  }
}

# Output for VM roles
output "vm_roles" {
  value       = local.vm_roles_resolved
  description = "Resolved roles for each VM"
}

# Summary output for easy reference
output "vm_summary" {
  value = {
    for i in range(var.vm_count) :
    proxmox_virtual_environment_vm.ubuntu-vm[i].name => {
      id   = proxmox_virtual_environment_vm.ubuntu-vm[i].vm_id
      ip   = chomp(data.local_file.vm_ip[i].content)
      mac  = format("02:00:00:00:%02x:%02x", i + 1, i + 100)
      node = proxmox_virtual_environment_vm.ubuntu-vm[i].node_name
      role = local.vm_roles_resolved[proxmox_virtual_environment_vm.ubuntu-vm[i].name]
    }
  }
  depends_on = [data.local_file.vm_ip]
}
