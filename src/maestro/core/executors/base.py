from abc import ABC, abstractmethod
from maestro.core.task import Task

class BaseExecutor(ABC):
    @abstractmethod
    def execute(self, task: Task):
        pass
