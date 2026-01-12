variable "proxmox_api_url" {
  description = "URL API di Proxmox"
  type        = string
  default     = "https://192.168.1.100:8006/api2/json"
}

variable "proxmox_user" {
  description = "Username Proxmox"
  type        = string
  default     = "root@pam"
}

variable "proxmox_host_user" {
  description = "Username Proxmox"
  type        = string
  default     = "root"
}

variable "proxmox_password" {
  description = "Password Proxmox"
  type        = string
  sensitive   = true
}

variable "target_node" {
  description = "Nome del nodo Proxmox"
  type        = string
  default     = "default" # Updated to match your node name
}

variable "template_name" {
  description = "Nome del template cloud-init da clonare"
  type        = string
  default     = "ubuntu-small" # Updated to match your template
}

variable "template_id" {
  description = "ID del template cloud-init da clonare (richiesto per BPG provider)"
  type        = number
  default     = 109 # Aggiorna questo con l'ID del tuo template
}

# New variable for number of VMs
variable "vm_count" {
  description = "Numero di VM da creare"
  type        = number
  default     = 1

  validation {
    condition     = var.vm_count >= 1 && var.vm_count <= 50
    error_message = "Il numero di VM deve essere compreso tra 1 e 50."
  }
}

# Changed from vm_name to vm_name_prefix
variable "vm_name_prefix" {
  description = "Prefisso per i nomi delle VM (sarà seguito da un numero)"
  type        = string
  default     = "terraform-vm"

  validation {
    condition     = can(regex("^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]$", var.vm_name_prefix))
    error_message = "Il prefisso del nome VM deve iniziare e finire con caratteri alfanumerici e può contenere solo lettere, numeri e trattini."
  }
}

# Network configuration (only if you want to override template settings)
variable "network_config" {
  description = "Configurazione IP (DHCP o IP statico)"
  type        = string
  default     = "ip=dhcp" # Keep template default
}

variable "ci_user" {
  description = "Username per cloud-init"
  type        = string
  default     = "ubuntu" # do not use user as it is excluded from ssh access
}

variable "ci_password" {
  description = "Password per cloud-init"
  type        = string
  default     = "password123"
  sensitive   = true
}

variable "ssh_key_path" {
  description = "Percorso della chiave SSH pubblica"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "disk_format" {
  description = "Formato del disco (raw, qcow2)"
  type        = string
  default     = "qcow2" # Template uses qcow2 format
}

variable "disk_slot" {
  description = "Slot del disco (es: scsi0, virtio0, sata0)"
  type        = string
  default     = "scsi0" # Template uses scsi0
}

variable "network_id" {
  description = "ID dell'interfaccia di rete"
  type        = number
  default     = 0
}

variable "network_model" {
  description = "Modello dell'interfaccia di rete"
  type        = string
  default     = "virtio" # Template uses virtio
}

variable "scsihw" {
  description = "Tipo di controller SCSI"
  type        = string
  default     = "virtio-scsi-single" # Template uses VirtIO SCSI single
}

variable "machine_type" {
  description = "Tipo di macchina QEMU"
  type        = string
  default     = "pc" # Default (i440fx) in Proxmox GUI = "pc" in Terraform

  validation {
    condition     = contains(["pc", "q35", "virt"], var.machine_type)
    error_message = "Il tipo di macchina deve essere uno tra: pc, q35, virt."
  }
}

variable "proxmox_host" {
  description = "Hostname Proxmox"
  type        = string
  default     = "localhost" # Updated to match your Proxmox host
}

# Optional: Advanced configuration for different VM configurations
variable "vm_configs" {
  description = "Configurazioni specifiche per VM individuali (opzionale)"
  type = map(object({
    cores     = optional(number)
    sockets   = optional(number)
    memory    = optional(number)
    disk_size = optional(string)
  }))
  default = {}
}

# Optional: Staggered deployment delay
variable "deployment_delay" {
  description = "Ritardo in secondi tra la creazione di ciascuna VM (per evitare sovraccarico)"
  type        = number
  default     = 180

  validation {
    condition     = var.deployment_delay >= 0 && var.deployment_delay <= 300
    error_message = "Il ritardo di deployment deve essere compreso tra 0 e 300 secondi."
  }
}

# VM Role Configuration
variable "vm_roles" {
  description = "Mappa dei ruoli per ciascuna VM. Ogni VM può avere ruolo 'k3s' o 'docker'. Se non specificato, default è 'k3s'."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for role in values(var.vm_roles) : contains(["k3s", "docker"], role)
    ])
    error_message = "I ruoli VM devono essere 'k3s' o 'docker'."
  }
}

# Default role for VMs not specified in vm_roles
variable "default_vm_role" {
  description = "Ruolo predefinito per le VM non specificate nella mappa vm_roles"
  type        = string
  default     = "k3s"

  validation {
    condition     = contains(["k3s", "docker"], var.default_vm_role)
    error_message = "Il ruolo predefinito deve essere 'k3s' o 'docker'."
  }
}
