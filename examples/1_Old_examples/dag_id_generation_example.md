# DAG ID Generation and Uniqueness

This document demonstrates the new DAG ID generation and uniqueness validation features in Maestro.

## Overview

Maestro now supports:
1. **Docker-like Random DAG ID Generation**: When no DAG ID is provided, Maestro generates a unique Docker-like name (e.g., `amazing_tesla`, `clever_newton`)
2. **Custom DAG ID Specification**: Users can specify their own DAG ID when submitting a DAG
3. **Uniqueness Validation**: Ensures no duplicate DAG IDs exist in the system
4. **Format Validation**: Validates that DAG IDs only contain alphanumeric characters, underscores, and hyphens

## API Usage Examples

### 1. Submit DAG with Auto-Generated ID

```bash
# Submit a DAG without specifying a DAG ID - Maestro will generate a unique one
curl -X POST "http://localhost:8000/dags/submit" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_file_path": "/path/to/my/dag.yaml",
    "resume": false,
    "fail_fast": true
  }'
```

**Response:**
```json
{
  "dag_id": "amazing_tesla",
  "execution_id": "12345678-1234-5678-9012-123456789012",
  "status": "submitted",
  "submitted_at": "2024-01-20T10:30:00",
  "message": "DAG amazing_tesla submitted successfully"
}
```

### 2. Submit DAG with Custom ID

```bash
# Submit a DAG with a custom DAG ID
curl -X POST "http://localhost:8000/dags/submit" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_file_path": "/path/to/my/dag.yaml",
    "dag_id": "my_custom_dag_v2",
    "resume": false,
    "fail_fast": true
  }'
```

**Response:**
```json
{
  "dag_id": "my_custom_dag_v2",
  "execution_id": "12345678-1234-5678-9012-123456789012",
  "status": "submitted",
  "submitted_at": "2024-01-20T10:30:00",
  "message": "DAG my_custom_dag_v2 submitted successfully"
}
```

### 3. Error Cases

#### Invalid DAG ID Format
```bash
curl -X POST "http://localhost:8000/dags/submit" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_file_path": "/path/to/my/dag.yaml",
    "dag_id": "invalid dag with spaces"
  }'
```

**Response:**
```json
{
  "detail": "Invalid DAG ID format: 'invalid dag with spaces'. Must contain only alphanumeric characters, underscores, and hyphens."
}
```

#### Duplicate DAG ID
```bash
curl -X POST "http://localhost:8000/dags/submit" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_file_path": "/path/to/my/dag.yaml",
    "dag_id": "existing_dag_id"
  }'
```

**Response:**
```json
{
  "detail": "DAG ID 'existing_dag_id' already exists. Please choose a different DAG ID."
}
```

## DAG ID Format Rules

### Valid DAG ID Examples:
- `simple_dag`
- `dag-with-hyphens`
- `dag_with_123_numbers`
- `CamelCaseDAG`
- `my-deployment-v1_2`
- `test_dag_001`

### Invalid DAG ID Examples:
- `dag with spaces` (contains spaces)
- `dag@with@symbols` (contains special characters)
- `dag.with.dots` (contains dots)
- `dag/with/slashes` (contains slashes)
- `dag#with#hash` (contains hash symbols)
- `` (empty string)

## Docker-Like Name Generation

Maestro uses a curated list of adjectives and nouns (similar to Docker's container naming) to generate unique, memorable DAG IDs:

### Example Generated Names:
- `amazing_tesla`
- `clever_newton`
- `bold_curie`
- `happy_einstein`
- `elegant_lovelace`
- `inspiring_hopper`
- `peaceful_hawking`
- `vibrant_darwin`

### Uniqueness Guarantee:
- Maestro attempts up to 100 random combinations
- If all attempts result in duplicates, a random suffix is added
- Format: `base_name_randomsuffix` (e.g., `amazing_tesla_a1b2c3`)

## Python Client Usage

If you're using the Python client, you can submit DAGs with custom or auto-generated IDs:

```python
from maestro.client.api_client import APIClient

client = APIClient("http://localhost:8000")

# Submit with auto-generated ID
response = client.submit_dag(
    dag_file_path="/path/to/my/dag.yaml",
    resume=False,
    fail_fast=True
)
print(f"Generated DAG ID: {response['dag_id']}")

# Submit with custom ID
response = client.submit_dag(
    dag_file_path="/path/to/my/dag.yaml",
    dag_id="my_custom_dag_v2",
    resume=False,
    fail_fast=True
)
print(f"Custom DAG ID: {response['dag_id']}")
```

## Migration Notes

### Backward Compatibility
- Existing DAG submissions without `dag_id` will continue to work
- Previously, DAG IDs were derived from the filename; now they're auto-generated
- If you need consistent DAG IDs, explicitly specify them in your submissions

### Best Practices
1. **Use descriptive custom IDs** for production workflows
2. **Let Maestro generate IDs** for ad-hoc testing and development
3. **Use versioning** in custom IDs (e.g., `data_pipeline_v1_2`)
4. **Follow naming conventions** that match your team's standards

## Monitoring DAG IDs

You can list all DAGs and their IDs using:

```bash
# List all DAGs
curl -X GET "http://localhost:8000/dags/list"

# List DAGs with specific status
curl -X GET "http://localhost:8000/dags/list?status=completed"
```

This will show you all DAG IDs in the system, both auto-generated and custom ones.
