# /home/dave-crd/Scrivania/Maestro/src/maestro/server/internals/

import typing

from maestro.server.tasks import BaseTask


class TaskRegistry:
    """Registry for task types with plugin discovery."""

    def __init__(self):
        self._task_types: typing.Dict[str, typing.Type[BaseTask]] = {}
        self._load_builtin_tasks()

    def _load_builtin_tasks(self):
        """Load built-in task types."""
        from maestro.server.tasks.ansible_task import AnsibleTask
        from maestro.server.tasks.bash_task import BashTask
        from maestro.server.tasks.extended_terraform_task import ExtendedTerraformTask
        from maestro.server.tasks.file_writer_task import FileWriterTask
        from maestro.server.tasks.print_task import PrintTask
        from maestro.server.tasks.python_task import PythonTask
        from maestro.server.tasks.terraform_task import TerraformTask
        from maestro.server.tasks.wait_task import WaitTask

        self.register("PrintTask", PrintTask)
        self.register("FileWriterTask", FileWriterTask)
        self.register("WaitTask", WaitTask)
        self.register("TerraformTask", TerraformTask)
        self.register("AnsibleTask", AnsibleTask)
        self.register("ExtendedTerraformTask", ExtendedTerraformTask)
        self.register("PythonTask", PythonTask)
        self.register("BashTask", BashTask)

    def register(self, name: str, task_class: typing.Type[BaseTask]):
        """Register a task type."""
        if not issubclass(task_class, BaseTask):
            raise ValueError(f"Task class {task_class} must inherit from BaseTask")
        self._task_types[name] = task_class

    def get(self, name: str) -> typing.Optional[typing.Type[BaseTask]]:
        """Get a task type by name."""
        return self._task_types.get(name)

    def list_types(self) -> typing.Dict[str, typing.Type[BaseTask]]:
        """List all registered task types."""
        return self._task_types.copy()
