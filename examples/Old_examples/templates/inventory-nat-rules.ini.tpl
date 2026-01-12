[proxmox_hosts]
${proxmox_host} ansible_host=${proxmox_host} ansible_user=${proxmox_user} ansible_ssh_private_key_file=${proxmox_ssh_key}

[proxmox_hosts:vars]
interfaces_file=/etc/network/interfaces
target_interface=${target_interface}
source_interface=${source_interface}
ansible_ssh_common_args='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
ansible_python_interpreter=/usr/bin/python3

[port_mappings]
%{ for service in vm_services ~}
${service.name} vm_id=${service.vm_id} vm_name=${service.vm_name} vm_ip=${service.vm_ip} vm_port=${service.vm_port} service=${service.service}%{ if service.vm_user != "" } vm_user=${service.vm_user}%{ endif } vm_role=${service.vm_role}
%{ endfor ~}
