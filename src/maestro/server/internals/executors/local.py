from maestro.server.internals.executors.base import BaseExecutor
from maestro.shared.task import Task

class LocalExecutor(BaseExecutor):
    def execute(self, task: Task):
        task.execute_local()
