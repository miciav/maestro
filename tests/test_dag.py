
import pytest
from datetime import datetime, timedelta

from maestro.shared.dag import DAG
from maestro.shared.task import Task

class DummyTask(Task):
    def execute_local(self):
        pass

def test_dag_add_task():
    dag = DAG()
    task = DummyTask(task_id="test_task")
    dag.add_task(task)
    assert "test_task" in dag.tasks

def test_dag_validation():
    dag = DAG()
    task1 = DummyTask(task_id="task1")
    task2 = DummyTask(task_id="task2", dependencies=["task1"])
    dag.add_task(task1)
    dag.add_task(task2)
    dag.validate()

def test_dag_cycle_detection():
    dag = DAG()
    task1 = DummyTask(task_id="task1", dependencies=["task3"])
    task2 = DummyTask(task_id="task2", dependencies=["task1"])
    task3 = DummyTask(task_id="task3", dependencies=["task2"])
    dag.add_task(task1)
    dag.add_task(task2)
    dag.add_task(task3)
    with pytest.raises(ValueError, match="DAG has a cycle."):
        dag.validate()

def test_dag_with_start_time():
    """Test DAG creation with start_time parameter."""
    start_time = datetime(2024, 1, 1, 9, 0, 0)
    dag = DAG(dag_id="test_dag", start_time=start_time)
    
    assert dag.dag_id == "test_dag"
    assert dag.start_time == start_time

def test_dag_without_start_time():
    """Test DAG creation without start_time parameter."""
    dag = DAG(dag_id="test_dag")
    
    assert dag.dag_id == "test_dag"
    assert dag.start_time is None

def test_dag_is_ready_to_start_future():
    """Test DAG readiness check with future start time."""
    future_time = datetime.now() + timedelta(hours=1)
    dag = DAG(dag_id="test_dag", start_time=future_time)
    
    assert not dag.is_ready_to_start()
    assert dag.time_until_start() > 0

def test_dag_is_ready_to_start_past():
    """Test DAG readiness check with past start time."""
    past_time = datetime.now() - timedelta(hours=1)
    dag = DAG(dag_id="test_dag", start_time=past_time)
    
    assert dag.is_ready_to_start()
    assert dag.time_until_start() == 0

def test_dag_is_ready_to_start_no_time():
    """Test DAG readiness check without start time."""
    dag = DAG(dag_id="test_dag")
    
    assert dag.is_ready_to_start()
    assert dag.time_until_start() is None

def test_dag_with_cron_schedule():
    """Test DAG creation with cron schedule."""
    cron_schedule = "0 9 * * *"  # Every day at 9:00 AM
    dag = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    
    assert dag.dag_id == "test_dag"
    assert dag.cron_schedule == cron_schedule
    assert dag.start_time is None

def test_dag_with_invalid_cron_schedule():
    """Test DAG creation with invalid cron schedule."""
    invalid_cron = "invalid cron"
    
    with pytest.raises(ValueError, match="Invalid cron expression"):
        DAG(dag_id="test_dag", cron_schedule=invalid_cron)

def test_dag_with_both_start_time_and_cron():
    """Test DAG creation with both start_time and cron_schedule (should fail)."""
    start_time = datetime(2024, 1, 1, 9, 0, 0)
    cron_schedule = "0 9 * * *"
    
    with pytest.raises(ValueError, match="Cannot specify both start_time and cron_schedule"):
        DAG(dag_id="test_dag", start_time=start_time, cron_schedule=cron_schedule)

def test_dag_cron_schedule_readiness():
    """Test DAG readiness check with cron schedule."""
    # Test with a cron that runs every minute
    cron_schedule = "* * * * *"
    dag = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    
    # Should be ready most of the time since it runs every minute
    current_time = datetime.now()
    assert dag.is_ready_to_start(current_time)
    
    # Test with specific time
    test_time = datetime(2024, 1, 1, 9, 0, 30)  # 30 seconds after 9:00 AM
    assert dag.is_ready_to_start(test_time)

def test_dag_cron_next_run_time():
    """Test getting next run time for cron scheduled DAG."""
    cron_schedule = "0 9 * * *"  # Every day at 9:00 AM
    dag = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    
    test_time = datetime(2024, 1, 1, 8, 0, 0)  # 8:00 AM
    next_run = dag.get_next_run_time(test_time)
    
    assert next_run is not None
    assert next_run.hour == 9
    assert next_run.minute == 0

def test_dag_schedule_descriptions():
    """Test schedule description methods."""
    # Test with start_time
    start_time = datetime(2024, 1, 1, 9, 0, 0)
    dag_start_time = DAG(dag_id="test_dag", start_time=start_time)
    assert "One-time execution" in dag_start_time.get_schedule_description()
    
    # Test with cron schedule
    cron_schedule = "0 9 * * *"
    dag_cron = DAG(dag_id="test_dag", cron_schedule=cron_schedule)
    assert "Cron schedule" in dag_cron.get_schedule_description()
    
    # Test with no schedule
    dag_no_schedule = DAG(dag_id="test_dag")
    assert "No schedule" in dag_no_schedule.get_schedule_description()
