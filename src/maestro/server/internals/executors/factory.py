from typing import Type, Dict

from maestro.server.internals.executors.base import BaseExecutor
from maestro.server.internals.executors.local import LocalExecutor
from maestro.server.internals.executors.ssh import SshExecutor
from maestro.server.internals.executors.docker import DockerExecutor
from maestro.server.internals.executors.kubernetes import KubernetesExecutor

class ExecutorFactory:
    def __init__(self):
        self._executors: Dict[str, Type[BaseExecutor]] = {
            "local": LocalExecutor,
            "ssh": SshExecutor,
            "docker": DockerExecutor,
            "kubernetes": KubernetesExecutor,
        }

    def register_executor(self, name: str, executor_class: Type[BaseExecutor]):
        self._executors[name] = executor_class

    def get_executor(self, name: str) -> BaseExecutor:
        executor_class = self._executors.get(name)
        if not executor_class:
            raise ValueError(f"Unknown executor: {name}")
        return executor_class()
