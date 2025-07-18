import pytest
from datetime import datetime, timedelta
from maestro.core.dag import DAG

def test_valid_cron_expression():
    cron_schedule = "0 9 * * *"  # Every day at 9:00 AM
    dag = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    assert dag.cron_schedule == cron_schedule

def test_invalid_cron_expression():
    invalid_cron = "invalid cron"
    with pytest.raises(ValueError, match="Invalid cron expression"):
        DAG(dag_id="test_dag", cron_schedule=invalid_cron)

def test_dag_ready_to_start():
    cron_schedule = "* * * * *"  # Every minute
    dag = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    now = datetime.now()
    assert dag.is_ready_to_start(now)

def test_dag_not_ready_to_start():
    cron_schedule = "0 9 * * *"  # Every day at 9:00 AM
    dag = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    now = datetime.now().replace(hour=10)
    assert not dag.is_ready_to_start(now)

def test_dag_next_run_time():
    cron_schedule = "0 9 * * *"  # Every day at 9:00 AM
    dag = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    now = datetime(2024, 1, 1, 8, 0, 0)  # 8:00 AM
    next_run = dag.get_next_run_time(now)
    assert next_run.hour == 9
    assert next_run.minute == 0

def test_dag_next_run_time_wrap_around():
    cron_schedule = "0 9 * * *"  # Every day at 9:00 AM
    dag = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    now = datetime(2024, 1, 1, 10, 0, 0)  # After the run time
    next_run = dag.get_next_run_time(now)
    assert next_run.hour == 9
    assert next_run.day == 2  # Next day

