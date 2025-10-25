# VM Creation Flow

Below is the sequential execution flow for VM deployment:

```
┌─────────────────┐    ┌─────────────────┐
│   VM 1 Created  │    │   VM 2 Created  │
└─────────────────┘    └─────────────────┘
         │                       │
         ▼                       │
┌─────────────────┐              │
│ VM 1 Wait Script│              │
│   (runs first)  │              │
└─────────────────┘              │
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌─────────────────┐
│ VM 1 Wait Done  │───▶│ VM 2 Wait Script│
│                 │    │   (runs second) │
└─────────────────┘    └─────────────────┘
```

Each VM's wait script runs in sequence, reducing the load on the Proxmox host and providing predictable deployment order.
