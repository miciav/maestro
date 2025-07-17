# Multi-Threaded DAG Execution Enhancement

## Overview

This enhancement adds multi-threaded execution capabilities to the Maestro task orchestrator, allowing users to run multiple DAGs concurrently while maintaining proper thread isolation and comprehensive lifecycle management.

## Key Features

### 1. Concurrent DAG Execution
- **Parallel Processing**: Multiple DAGs can be executed simultaneously in separate threads
- **Thread Isolation**: Each DAG runs in its own thread with proper resource isolation
- **Thread Pool Management**: Uses `ThreadPoolExecutor` for efficient thread management (max 10 concurrent DAGs)

### 2. Enhanced Database Layer
- **Thread-Safe Operations**: All database operations are protected with reentrant locks
- **Execution Tracking**: Comprehensive tracking of DAG executions with unique execution IDs
- **Timestamp Tracking**: Started and completed timestamps for both DAGs and individual tasks
- **Thread Identification**: Each operation is tagged with thread ID for debugging and monitoring

### 3. Advanced CLI Interface
A new `maestro-concurrent` CLI provides comprehensive DAG management:

#### Commands:
- `run-async`: Execute DAGs asynchronously in background threads
- `monitor`: Real-time monitoring of DAG execution with live updates
- `status`: Check status of specific DAGs or all running DAGs
- `logs`: View execution logs with thread identification
- `validate`: Validate DAG files and show structure
- `list-dags`: List all DAGs in the database with their execution history

## Architecture

### Core Components

#### 1. Enhanced StatusManager
- **Thread-Safe Database Operations**: All methods use reentrant locks
- **Enhanced Schema**: New tables for execution tracking and logging
- **Execution Management**: Create, update, and query DAG execution records
- **Logging Integration**: Centralized logging with thread identification

#### 2. Enhanced Orchestrator
- **Thread Pool Integration**: Uses `ThreadPoolExecutor` for concurrent execution
- **Execution ID Management**: Unique execution IDs for each DAG run
- **Thread-Safe Execution**: Proper database connection handling in threads
- **Enhanced Error Handling**: Comprehensive error management across threads

#### 3. Multi-Threaded CLI
- **Asynchronous Execution**: Background DAG execution with immediate feedback
- **Real-Time Monitoring**: Live status updates with Rich UI components
- **Comprehensive Status**: Detailed execution information and task tracking
- **Log Management**: Thread-aware logging and retrieval

## Database Schema

#### Enhanced Tables:
1. **task_status**: Task execution status with timestamps and thread IDs
   - `dag_id`: Unique identifier for the DAG
   - `task_id`: Unique identifier for the task within the DAG
   - `status`: Current status (pending, running, completed, failed)
   - `started_at`: Timestamp when task started
   - `completed_at`: Timestamp when task completed
   - `thread_id`: Thread identifier for debugging

2. **dag_executions**: DAG execution tracking with lifecycle management
   - `dag_id`: Unique identifier for the DAG
   - `execution_id`: Unique identifier for this execution instance
   - `status`: Current status (running, completed, failed, cancelled)
   - `started_at`: Timestamp when execution started
   - `completed_at`: Timestamp when execution completed
   - `thread_id`: Thread identifier for debugging
   - `pid`: Process identifier

3. **execution_logs**: Centralized logging with thread identification
   - `id`: Auto-incrementing primary key
   - `dag_id`: Associated DAG identifier
   - `execution_id`: Associated execution identifier
   - `task_id`: Associated task identifier (nullable)
   - `level`: Log level (INFO, WARNING, ERROR, DEBUG)
   - `message`: Log message content
   - `timestamp`: When the log was created
   - `thread_id`: Thread identifier for debugging

## Usage Examples

### Basic Concurrent Execution

```bash
# Start a DAG asynchronously
maestro-concurrent run-async my_dag.yaml

# Monitor execution in real-time
maestro-concurrent monitor my_dag_id

# Check status of all running DAGs
maestro-concurrent status

# View execution logs
maestro-concurrent logs my_dag_id
```

### Advanced Usage

```bash
# Resume a DAG from checkpoint
maestro-concurrent run-async my_dag.yaml --resume

# Run without failing fast
maestro-concurrent run-async my_dag.yaml --no-fail-fast

# Monitor specific execution
maestro-concurrent monitor my_dag_id --execution-id abc123

# View limited logs
maestro-concurrent logs my_dag_id --limit 50

# Show summary statistics
maestro-concurrent summary

# List all DAGs
maestro-concurrent list-dags

# List only failed DAGs
maestro-concurrent list-dags --status failed

# Show execution history for a specific DAG
maestro-concurrent history my_dag_id --limit 5

# Clean up old executions (dry run)
maestro-concurrent cleanup --days 30 --dry-run

# Cancel a running DAG
maestro-concurrent cancel my_dag_id
```

### Database Management

```bash
# View comprehensive statistics
maestro-concurrent summary

# Filter DAGs by status
maestro-concurrent list-dags --status running
maestro-concurrent list-dags --status completed
maestro-concurrent list-dags --status failed
maestro-concurrent list-dags --status cancelled

# View execution history with duration calculations
maestro-concurrent history my_dag_id

# Clean up old records
maestro-concurrent cleanup --days 7  # Remove records older than 7 days

# Cancel specific execution
maestro-concurrent cancel my_dag_id --execution-id abc123
```

## Technical Implementation

### Thread Safety
- **Reentrant Locks**: All database operations use `threading.RLock()`
- **Connection Management**: Proper SQLite connection handling with WAL mode
- **Atomic Operations**: Database transactions ensure data consistency

### Resource Management
- **Thread Pool**: Limited to 10 concurrent threads to prevent resource exhaustion
- **Connection Pooling**: Efficient database connection management
- **Cleanup**: Proper resource cleanup on thread completion

### Error Handling
- **Thread-Specific Errors**: Errors are isolated to individual threads
- **Comprehensive Logging**: All errors are logged with thread context
- **Graceful Degradation**: Failed DAGs don't affect other running DAGs

## Testing

All existing tests pass with the new multi-threaded implementation:
- Database persistence tests
- DAG execution tests
- Error handling tests
- Resume functionality tests

## Benefits

1. **Increased Throughput**: Multiple DAGs can execute simultaneously
2. **Better Resource Utilization**: Efficient use of system resources
3. **Improved User Experience**: Real-time monitoring and status updates
4. **Enhanced Debugging**: Thread-aware logging and comprehensive status tracking
5. **Scalability**: Foundation for future distributed execution capabilities

## Future Enhancements

1. **Distributed Execution**: Extend to multiple nodes
2. **Resource Limits**: Per-DAG resource constraints
3. **Priority Scheduling**: DAG execution prioritization
4. **Web Interface**: Browser-based monitoring and control
5. **Metrics Collection**: Performance and execution metrics

## Backward Compatibility

The enhancement maintains full backward compatibility:
- Existing CLI commands continue to work unchanged
- Database schema is automatically upgraded
- All existing functionality remains intact
- New features are opt-in through the new CLI interface
