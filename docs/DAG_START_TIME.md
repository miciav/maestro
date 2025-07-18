# DAG Scheduling Feature

## Overview

The DAG Scheduling feature allows you to specify when a DAG should begin execution. This is useful for scheduling DAGs to run at specific times, such as during off-peak hours or at predetermined intervals. The feature supports both one-time execution at a specific datetime and recurring execution using cron expressions.

## Configuration

### YAML Configuration

You can schedule DAGs using either a one-time `start_time` or a recurring `cron_schedule`:

#### One-time Execution

```yaml
dag:
  name: "my_scheduled_dag"
  start_time: "2024-01-01T09:00:00"  # DAG will start at 9:00 AM on January 1st, 2024
  tasks:
    - task_id: "morning_task"
      type: "PrintTask"
      params:
        message: "Good morning! This runs at the scheduled time."
      dependencies: []
```

#### Recurring Execution with Cron

```yaml
dag:
  name: "daily_report_dag"
  cron_schedule: "0 9 * * *"  # Every day at 9:00 AM
  tasks:
    - task_id: "generate_report"
      type: "ReportTask"
      params:
        report_type: "daily"
      dependencies: []
    - task_id: "send_email"
      type: "EmailTask"
      dependencies: ["generate_report"]
```

### Supported Date/Time Formats

The `start_time` parameter supports multiple datetime formats:

- `2024-01-01 09:00:00` - Standard datetime format
- `2024-01-01T09:00:00` - ISO 8601 format
- `2024-01-01T09:00:00Z` - ISO 8601 format with UTC timezone
- `2024-01-01 09:00` - Date and time without seconds
- `2024-01-01T09:00` - ISO date and time without seconds
- `2024-01-01` - Date only (assumes 00:00:00)

### Cron Expression Format

The `cron_schedule` parameter accepts standard cron expressions with 5 fields:

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of week (0 - 6) (Sunday to Saturday)
│ │ │ │ │
* * * * *
```

#### Common Cron Examples

| Expression | Description |
|------------|-------------|
| `0 9 * * *` | Every day at 9:00 AM |
| `*/15 * * * *` | Every 15 minutes |
| `0 */6 * * *` | Every 6 hours |
| `0 0 * * 0` | Every Sunday at midnight |
| `30 2 * * 1-5` | Every weekday at 2:30 AM |
| `0 0 1 * *` | First day of every month at midnight |
| `0 9-17 * * 1-5` | Every hour from 9 AM to 5 PM, Monday to Friday |
| `0 8 * * 1,3,5` | Every Monday, Wednesday, and Friday at 8 AM |

## Programming Interface

### DAG Class

The `DAG` class now accepts optional `start_time` or `cron_schedule` parameters:

```python
from datetime import datetime
from maestro.core.dag import DAG

# Create a DAG with a specific start time (one-time execution)
start_time = datetime(2024, 1, 1, 9, 0, 0)
dag1 = DAG(dag_id="one_time_dag", start_time=start_time)

# Create a DAG with a cron schedule (recurring execution)
dag2 = DAG(dag_id="recurring_dag", cron_schedule="0 9 * * *")

# Check if the DAG is ready to start
if dag1.is_ready_to_start():
    print("DAG is ready to start!")
else:
    print(f"DAG will start in {dag1.time_until_start():.2f} seconds")

# Get schedule information
print(f"Schedule description: {dag2.get_schedule_description()}")
print(f"Next run time: {dag2.get_next_run_time()}")
```

### Available Methods

The `DAG` class provides these methods for working with scheduling:

- `is_ready_to_start(current_time: Optional[datetime] = None) -> bool`: Returns `True` if the DAG can start now
- `time_until_start(current_time: Optional[datetime] = None) -> Optional[float]`: Returns the number of seconds until the DAG can start, or `None` if no scheduling is set
- `get_next_run_time(current_time: Optional[datetime] = None) -> Optional[datetime]`: Returns the next scheduled execution time
- `get_schedule_description() -> str`: Returns a human-readable description of the schedule

## Behavior

### When start_time is set (one-time execution):
- If the current time is **before** the start time: `is_ready_to_start()` returns `False`
- If the current time is **after** the start time: `is_ready_to_start()` returns `True`
- `time_until_start()` returns the number of seconds until the start time (or 0 if in the past)

### When cron_schedule is set (recurring execution):
- `is_ready_to_start()` returns `True` if the current time is within 60 seconds of a scheduled execution time
- `time_until_start()` returns the number of seconds until the next scheduled execution
- `get_next_run_time()` returns the next datetime when the DAG should run

### When neither is set:
- `is_ready_to_start()` always returns `True`
- `time_until_start()` returns `None`
- `get_next_run_time()` returns `None`

### Important Notes:
- You cannot specify both `start_time` and `cron_schedule` - this will raise a `ValueError`
- Cron expressions are validated when creating the DAG - invalid expressions will raise a `ValueError`

## Examples

### One-time Scheduled DAG

```yaml
dag:
  name: "daily_backup"
  start_time: "2024-01-01T02:00:00"  # Run at 2 AM
  tasks:
    - task_id: "backup_database"
      type: "BackupTask"
      dependencies: []
    - task_id: "cleanup_old_backups"
      type: "CleanupTask"
      dependencies: ["backup_database"]
```

### Recurring Cron Scheduled DAGs

#### Daily Report at 9 AM

```yaml
dag:
  name: "daily_report"
  cron_schedule: "0 9 * * *"  # Every day at 9:00 AM
  tasks:
    - task_id: "generate_report"
      type: "ReportTask"
      dependencies: []
    - task_id: "send_email"
      type: "EmailTask"
      dependencies: ["generate_report"]
```

#### System Health Check Every 15 Minutes

```yaml
dag:
  name: "health_check"
  cron_schedule: "*/15 * * * *"  # Every 15 minutes
  tasks:
    - task_id: "check_services"
      type: "HealthCheckTask"
      dependencies: []
    - task_id: "alert_if_down"
      type: "AlertTask"
      dependencies: ["check_services"]
```

#### Weekly Report Every Sunday

```yaml
dag:
  name: "weekly_report"
  cron_schedule: "0 0 * * 0"  # Every Sunday at midnight
  tasks:
    - task_id: "compile_weekly_stats"
      type: "StatsTask"
      dependencies: []
    - task_id: "generate_weekly_report"
      type: "ReportTask"
      dependencies: ["compile_weekly_stats"]
```

### Programmatic Usage

```python
from datetime import datetime, timedelta
from maestro.core.dag import DAG

# Schedule a DAG to run in 1 hour (one-time)
future_time = datetime.now() + timedelta(hours=1)
dag_one_time = DAG(dag_id="delayed_dag", start_time=future_time)

# Create a recurring DAG (every hour)
dag_recurring = DAG(dag_id="hourly_dag", cron_schedule="0 * * * *")

# Check readiness
if dag_one_time.is_ready_to_start():
    # Execute the DAG
    orchestrator.run_dag(dag_one_time)
else:
    print(f"Waiting {dag_one_time.time_until_start():.0f} seconds before starting...")

# Get information about the recurring DAG
print(f"Recurring DAG schedule: {dag_recurring.get_schedule_description()}")
print(f"Next run: {dag_recurring.get_next_run_time()}")
print(f"Time until next run: {dag_recurring.time_until_start():.0f} seconds")
```

## Integration with Orchestrator

The orchestrator can use the start time information to implement scheduling logic:

```python
from maestro.core.orchestrator import Orchestrator

orchestrator = Orchestrator()
dag = orchestrator.load_dag_from_file("scheduled_dag.yaml")

# Only run if the DAG is ready
if dag.is_ready_to_start():
    orchestrator.run_dag(dag)
else:
    print(f"DAG '{dag.dag_id}' is scheduled to start at {dag.start_time}")
```

## Error Handling

If an invalid date format is provided, the DAG loader will raise a `ValueError`:

```python
# This will raise ValueError: Invalid start_time format
dag = load_dag_from_file("dag_with_invalid_time.yaml")
```

## Backward Compatibility

This feature is fully backward compatible. Existing DAG configurations without the `start_time` parameter will continue to work as before, with the DAG being ready to start immediately.
