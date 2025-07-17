from maestro.core.executors.base import BaseExecutor
from maestro.core.task import Task

class SshExecutor(BaseExecutor):
    def execute(self, task: Task):
        # Placeholder for SSH execution logic
        print(f"Executing task {task.task_id} via SSH")
        task.execute_local() # For now, just call local execution
