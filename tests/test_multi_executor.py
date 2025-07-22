import pytest
import time
from unittest.mock import Mock, patch, MagicMock

from maestro.server.internals.orchestrator import Orchestrator
from maestro.shared.dag import DAG
from maestro.tasks.base import BaseTask
from maestro.shared.task import TaskStatus
from maestro.server.internals.executors.factory import ExecutorFactory
from maestro.server.internals.executors.local import LocalExecutor
from maestro.server.internals.executors.base import BaseExecutor


class MultiExecutorTestTask(BaseTask):
    """Test task for multi-executor testing."""
    executed: bool = False
    executor_used: str = None

    def execute_local(self):
        self.executed = True
        self.executor_used = "local"


class CustomExecutor(BaseExecutor):
    """Custom executor for testing."""
    def __init__(self):
        self.executed_tasks = []
    
    def execute(self, task):
        self.executed_tasks.append(task.task_id)
        task.execute_local()


@pytest.fixture
def orchestrator_with_executors(tmp_path):
    """Create orchestrator with multiple executors."""
    db_path = tmp_path / "test_multi_executor.db"
    orchestrator = Orchestrator(log_level="CRITICAL", db_path=str(db_path))
    orchestrator.register_task_type("test_task", MultiExecutorTestTask)
    return orchestrator


@pytest.fixture
def dag_with_different_executors():
    """Create DAG with tasks using different executors."""
    dag = DAG(dag_id="multi_executor_dag")
    
    # Tasks with different executors
    task1 = MultiExecutorTestTask(task_id="local_task", executor="local")
    task2 = MultiExecutorTestTask(task_id="ssh_task", executor="ssh")
    task3 = MultiExecutorTestTask(task_id="docker_task", executor="docker")
    
    dag.add_task(task1)
    dag.add_task(task2)
    dag.add_task(task3)
    
    return dag


class TestMultiExecutorSupport:
    """Test multi-executor support functionality."""
    
    def test_executor_factory_default_executors(self):
        """Test that ExecutorFactory has default executors registered."""
        factory = ExecutorFactory()
        
        # Test getting default executors
        local_executor = factory.get_executor("local")
        assert isinstance(local_executor, LocalExecutor)
        
        # Test that unknown executor raises error
        with pytest.raises(ValueError, match="Unknown executor: unknown"):
            factory.get_executor("unknown")
    
    def test_executor_factory_register_custom_executor(self):
        """Test registering custom executor."""
        factory = ExecutorFactory()
        
        # Register custom executor
        factory.register_executor("custom", CustomExecutor)
        
        # Test getting custom executor
        custom_executor = factory.get_executor("custom")
        assert isinstance(custom_executor, CustomExecutor)
    
    def test_task_executor_field_default(self):
        """Test that task executor field defaults to 'local'."""
        task = MultiExecutorTestTask(task_id="test_task")
        assert task.executor == "local"
    
    def test_task_executor_field_custom(self):
        """Test setting custom executor for task."""
        task = MultiExecutorTestTask(task_id="test_task", executor="ssh")
        assert task.executor == "ssh"
    
    def test_dag_execution_with_different_executors(self, orchestrator_with_executors, dag_with_different_executors):
        """Test DAG execution with tasks using different executors."""
        # Create custom executors for testing
        class TestSshExecutor(BaseExecutor):
            def execute(self, task):
                task.execute_local()
                task.executor_used = "ssh"
        
        class TestDockerExecutor(BaseExecutor):
            def execute(self, task):
                task.execute_local()
                task.executor_used = "docker"
        
        # Register custom executors
        orchestrator_with_executors.executor_factory.register_executor("ssh", TestSshExecutor)
        orchestrator_with_executors.executor_factory.register_executor("docker", TestDockerExecutor)
        
        # Run the DAG
        orchestrator_with_executors.run_dag(dag_with_different_executors)
        
        # Verify all tasks were executed
        assert dag_with_different_executors.tasks["local_task"].executed
        assert dag_with_different_executors.tasks["ssh_task"].executed
        assert dag_with_different_executors.tasks["docker_task"].executed
        
        # Verify correct executors were used
        assert dag_with_different_executors.tasks["local_task"].executor_used == "local"
        assert dag_with_different_executors.tasks["ssh_task"].executor_used == "ssh"
        assert dag_with_different_executors.tasks["docker_task"].executor_used == "docker"
    
    def test_concurrent_execution_with_different_executors(self, orchestrator_with_executors):
        """Test concurrent execution with different executors."""
        dag = DAG(dag_id="concurrent_multi_executor_dag")
        
        # Create tasks with different executors
        task1 = MultiExecutorTestTask(task_id="local_task1", executor="local")
        task2 = MultiExecutorTestTask(task_id="local_task2", executor="local")
        
        dag.add_task(task1)
        dag.add_task(task2)
        
        # Run concurrently
        execution_id = orchestrator_with_executors.run_dag_in_thread(dag)
        
        # Wait for completion
        time.sleep(0.2)
        
        # Verify execution completed
        with orchestrator_with_executors.status_manager as sm:
            details = sm.get_dag_execution_details(dag.dag_id, execution_id)
            assert details["status"] == "completed"
        
        # Verify tasks were executed
        assert dag.tasks["local_task1"].executed
        assert dag.tasks["local_task2"].executed
    
    def test_executor_failure_handling(self, orchestrator_with_executors):
        """Test handling of executor failures."""
        dag = DAG(dag_id="executor_failure_dag")
        
        # Create task with non-existent executor
        task = MultiExecutorTestTask(task_id="invalid_executor_task", executor="non_existent")
        dag.add_task(task)
        
        # Run should fail with unknown executor
        with pytest.raises(Exception, match="Unknown executor: non_existent"):
            orchestrator_with_executors.run_dag(dag)
    
    def test_custom_executor_registration_and_usage(self, orchestrator_with_executors):
        """Test registering and using custom executor."""
        # Register custom executor
        orchestrator_with_executors.executor_factory.register_executor("custom", CustomExecutor)
        
        # Create DAG with custom executor task
        dag = DAG(dag_id="custom_executor_dag")
        task = MultiExecutorTestTask(task_id="custom_task", executor="custom")
        dag.add_task(task)
        
        # Run DAG
        orchestrator_with_executors.run_dag(dag)
        
        # Verify task was executed
        assert task.executed
    
    def test_mixed_executor_dependencies(self, orchestrator_with_executors):
        """Test DAG with tasks using different executors and dependencies."""
        dag = DAG(dag_id="mixed_executor_dependencies_dag")
        
        # Create tasks with dependencies and different executors
        task1 = MultiExecutorTestTask(task_id="local_root", executor="local")
        task2 = MultiExecutorTestTask(task_id="local_dependent", executor="local", dependencies=["local_root"])
        
        dag.add_task(task1)
        dag.add_task(task2)
        
        # Run DAG
        orchestrator_with_executors.run_dag(dag)
        
        # Verify execution order and completion
        assert task1.executed
        assert task2.executed
        assert task1.status == TaskStatus.COMPLETED
        assert task2.status == TaskStatus.COMPLETED
    
    def test_executor_factory_thread_safety(self):
        """Test that ExecutorFactory is thread-safe."""
        from concurrent.futures import ThreadPoolExecutor as TPE
        
        factory = ExecutorFactory()
        
        def get_executor(name):
            return factory.get_executor(name)
        
        # Test concurrent access to executor factory
        with TPE(max_workers=5) as executor:
            futures = []
            for _ in range(10):
                future = executor.submit(get_executor, "local")
                futures.append(future)
            
            # All should complete without errors
            results = [f.result() for f in futures]
            
            # All should return LocalExecutor instances
            for result in results:
                assert isinstance(result, LocalExecutor)


class TestExecutorIntegration:
    """Test executor integration with the orchestrator."""
    
    def test_orchestrator_uses_correct_executor(self, orchestrator_with_executors):
        """Test that orchestrator uses the correct executor for each task."""
        dag = DAG(dag_id="executor_integration_dag")
        
        # Create tasks with specific executors
        local_task = MultiExecutorTestTask(task_id="local_task", executor="local")
        dag.add_task(local_task)
        
        # Mock the executor factory to track calls
        with patch.object(orchestrator_with_executors.executor_factory, 'get_executor', wraps=orchestrator_with_executors.executor_factory.get_executor) as mock_get_executor:
            orchestrator_with_executors.run_dag(dag)
            
            # Verify get_executor was called with correct executor name
            mock_get_executor.assert_called_with("local")
    
    def test_executor_context_isolation(self, orchestrator_with_executors):
        """Test that different executors don't interfere with each other."""
        # Register two custom executors
        executor1 = CustomExecutor()
        executor2 = CustomExecutor()
        
        orchestrator_with_executors.executor_factory.register_executor("custom1", lambda: executor1)
        orchestrator_with_executors.executor_factory.register_executor("custom2", lambda: executor2)
        
        dag = DAG(dag_id="executor_isolation_dag")
        
        task1 = MultiExecutorTestTask(task_id="task1", executor="custom1")
        task2 = MultiExecutorTestTask(task_id="task2", executor="custom2")
        
        dag.add_task(task1)
        dag.add_task(task2)
        
        # Run DAG
        orchestrator_with_executors.run_dag(dag)
        
        # Verify each executor only executed its own task
        assert "task1" in executor1.executed_tasks
        assert "task1" not in executor2.executed_tasks
        assert "task2" in executor2.executed_tasks
        assert "task2" not in executor1.executed_tasks
    
    def test_executor_error_propagation(self, orchestrator_with_executors):
        """Test that executor errors are properly propagated."""
        # Create a failing executor
        class FailingExecutor(BaseExecutor):
            def execute(self, task):
                raise Exception("Executor failed")
        
        orchestrator_with_executors.executor_factory.register_executor("failing", FailingExecutor)
        
        dag = DAG(dag_id="failing_executor_dag")
        task = MultiExecutorTestTask(task_id="failing_task", executor="failing")
        dag.add_task(task)
        
        # Run should fail with executor error
        with pytest.raises(Exception, match="Task failing_task failed: Executor failed"):
            orchestrator_with_executors.run_dag(dag)
        
        # Verify task status is marked as failed
        assert task.status == TaskStatus.FAILED
