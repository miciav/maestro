import typing

from maestro.tasks import BaseTask


class TaskRegistry:
    """Registry for task types with plugin discovery."""

    def __init__(self):
        self._task_types: typing.Dict[str, typing.Type[BaseTask]] = {}
        self._load_builtin_tasks()

    def _load_builtin_tasks(self):
        """Load built-in task types."""
        from maestro.tasks.print_task import PrintTask
        from maestro.tasks.file_writer_task import FileWriterTask
        from maestro.tasks.wait_task import WaitTask
        from maestro.tasks.terraform_task import TerraformTask
        from maestro.tasks.ansible_task import AnsibleTask
        from maestro.tasks.extended_terraform_task import ExtendedTerraformTask

        self.register("PrintTask", PrintTask)
        self.register("FileWriterTask", FileWriterTask)
        self.register("WaitTask", WaitTask)
        self.register("TerraformTask", TerraformTask)
        self.register("AnsibleTask", AnsibleTask)
        self.register("ExtendedTerraformTask", ExtendedTerraformTask)

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
