"""Expose server subpackages for importlib-based lookups.

The test-suite performs patching using dotted import paths such as
``maestro.server.internals.status_manager.StatusManager``.  The
``pkgutil.resolve_name`` helper that backs :func:`unittest.mock.patch`
expects the ``internals`` attribute to be available directly on the
``maestro.server`` package.  Explicitly importing the subpackages makes the
attributes available so that the ORM refactor package layout continues to
work with the existing tests.
"""

from . import api, internals, services, tasks

__all__ = [
    "api",
    "internals",
    "services",
    "tasks",
]
