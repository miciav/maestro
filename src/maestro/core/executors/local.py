from maestro.core.executors.base import BaseExecutor
from maestro.core.task import Task

class LocalExecutor(BaseExecutor):
    def execute(self, task: Task):
        task.execute_local()
