import pytest

from maestro.server.internals.task_registry import TaskRegistry
from maestro.server.tasks.base import BaseTask


def test_builtin_tasks_are_loaded():
    registry = TaskRegistry()
    types = registry.list_types()

    # Controlliamo che i task principali siano presenti
    expected = {
        "PrintTask",
        "FileWriterTask",
        "WaitTask",
        "TerraformTask",
        "AnsibleTask",
        "ExtendedTerraformTask",
        "PythonTask",
        "BashTask",
    }

    for name in expected:
        assert name in types, f"Expected built-in task '{name}' to be registered"


def test_register_valid_task():
    registry = TaskRegistry()

    class DummyTask(BaseTask):
        pass

    registry.register("DummyTask", DummyTask)

    assert registry.get("DummyTask") is DummyTask
    assert "DummyTask" in registry.list_types()


def test_register_invalid_task():
    registry = TaskRegistry()

    class NotATask:
        pass

    with pytest.raises(ValueError):
        registry.register("BadTask", NotATask)


def test_get_unknown_task_returns_none():
    registry = TaskRegistry()
    assert registry.get("UNKNOWN") is None


def test_list_types_returns_copy_not_reference():
    registry = TaskRegistry()
    types1 = registry.list_types()
    types2 = registry.list_types()

    # devono essere uguali come contenuto...
    assert types1 == types2

    # ...ma NON lo stesso oggetto
    assert types1 is not types2
