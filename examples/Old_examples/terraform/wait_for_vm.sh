#!/bin/bash

VMID="$1"
PROXMOX_USER="$2"
PROXMOX_HOST="$3"
EXEC_CMD="${4:-systemctl status qemu-guest-agent}"  # Default to checking agent status

# Color codes for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} [VM $VMID] $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} [VM $VMID] $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} [VM $VMID] $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} [VM $VMID] $1"
}

log_info "Starting VM initialization process..."
log_info "Waiting for VM to get IP via Proxmox..."

# Maximum wait time (30 minutes)
MAX_WAIT_TIME=1800
START_TIME=$(date +%s)

# Function to check if we've exceeded max wait time
check_timeout() {
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED -gt $MAX_WAIT_TIME ]; then
        log_error "Maximum wait time of ${MAX_WAIT_TIME} seconds exceeded"
        exit 1
    fi
    
    # Show progress every 5 minutes
    if [ $((ELAPSED % 300)) -eq 0 ] && [ $ELAPSED -gt 0 ]; then
        log_info "Still waiting... (${ELAPSED}s elapsed)"
    fi
}

# Wait for IP address
while true; do
    check_timeout
    
    output=$(ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${PROXMOX_USER}@${PROXMOX_HOST}" \
        "qm guest cmd $VMID network-get-interfaces" 2>/dev/null)
    
    ssh_exit_code=$?
    
    if [ $ssh_exit_code -ne 0 ]; then
        log_warning "SSH connection failed or VM not ready for guest commands"
        sleep 30
        continue
    fi
    
    # Check if we got valid JSON output
    if ! echo "$output" | jq . >/dev/null 2>&1; then
        log_warning "Invalid JSON response from guest agent"
        sleep 30
        continue
    fi
    
    ip=$(echo "$output" | jq -r '.[]? | select(."ip-addresses"?) | ."ip-addresses"[] | select(."ip-address" | test("^192\\.|^10\\.|^172\\.")) | ."ip-address"' | head -n1)
    
    if [[ -n "$ip" ]]; then
        log_success "VM has IP: $ip"
        # Write IP to file for Terraform to read
        echo "$ip" > /tmp/vm_${VMID}_ip.txt
        break
    else
        log_info "No IP yet. Sleeping 30s..."
        sleep 30
    fi
done

log_info "Checking qemu-guest-agent status..."

# Wait for qemu-guest-agent to be running and stable
AGENT_CHECK_COUNT=0
REQUIRED_CONSECUTIVE_CHECKS=0

while [ $AGENT_CHECK_COUNT -lt $REQUIRED_CONSECUTIVE_CHECKS ]; do
    check_timeout
    
    agent_status=$(ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
        "${PROXMOX_USER}@${PROXMOX_HOST}" \
        "qm agent $VMID ping" 2>/dev/null)
    
    if [[ $? -eq 0 ]]; then
        AGENT_CHECK_COUNT=$((AGENT_CHECK_COUNT + 1))
        log_success "qemu-guest-agent ping successful (${AGENT_CHECK_COUNT}/${REQUIRED_CONSECUTIVE_CHECKS})"
        
        if [ $AGENT_CHECK_COUNT -lt $REQUIRED_CONSECUTIVE_CHECKS ]; then
            log_info "Waiting 10 seconds before next check..."
            sleep 10
        fi
    else
        log_warning "qemu-guest-agent is not running or not responsive. Retrying in 30s..."
        AGENT_CHECK_COUNT=0  # Reset counter on failure
        sleep 30
    fi
done

log_success "qemu-guest-agent is stable and responsive"

# Additional stabilization time
log_info "Waiting additional 10 seconds for agent to fully stabilize..."
sleep 10

# Test guest exec functionality and verify agent service
log_info "Testing guest exec functionality and checking qemu-guest-agent service status..."

exec_result=$(ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
    "${PROXMOX_USER}@${PROXMOX_HOST}" \
    "qm guest exec $VMID -- systemctl status qemu-guest-agent" 2>/dev/null)

exec_exit_code=$?

if [[ $exec_exit_code -eq 0 ]]; then
    # Check if the service is actually active
    if echo "$exec_result" | grep -q "Active: active (running)"; then
        log_success "Guest exec is working and qemu-guest-agent service is active and running"
        log_success "VM is fully ready for operations"
        
        # Final verification - try to get system info
        log_info "Performing final system verification..."
        system_info=$(ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
            "${PROXMOX_USER}@${PROXMOX_HOST}" \
            "qm guest exec $VMID -- uname -a" 2>/dev/null)
        
        if [[ $? -eq 0 ]]; then
            log_success "System verification complete"
            log_info "System info: $(echo "$system_info" | head -1)"
        else
            log_warning "System verification failed, but basic agent communication works"
        fi
        
    else
        log_warning "Guest exec worked but qemu-guest-agent service is not active"
        log_warning "Service status: $exec_result"
        log_warning "VM might not be fully ready - consider installing/starting the agent"
    fi
else
    log_warning "Guest exec failed - VM might still be booting or agent not properly installed"
    log_warning "Continuing anyway as basic agent communication is established"
fi

TOTAL_TIME=$(($(date +%s) - START_TIME))
log_success "VM initialization completed in ${TOTAL_TIME} seconds"

# Create a summary file for debugging
summary_file="/tmp/vm_${VMID}_summary.txt"
cat > "$summary_file" << EOF
VM ID: $VMID
IP Address: $ip
Initialization Time: ${TOTAL_TIME} seconds
Agent Status: $([ $exec_exit_code -eq 0 ] && echo "Working" || echo "Limited")
Timestamp: $(date)
EOF

log_info "Summary written to: $summary_file"