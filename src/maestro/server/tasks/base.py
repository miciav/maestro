from enum import Enum
from typing import Optional

from pydantic import Field

from maestro.shared.task import Task


class DependencyPolicy(str, Enum):
    """
    Come interpretare lo stato delle dipendenze upstream per decidere se
    una task deve essere eseguita.

    Valori:
    - ALL  -> tutte le dipendenze devono essere COMPLETED (successo)
    - ANY  -> almeno una dipendenza deve essere COMPLETED
    - NONE -> esegui comunque quando tutte le dipendenze sono in stato terminale
    """

    ALL = "all"
    ANY = "any"
    NONE = "none"


class BaseTask(Task):
    """Base class for a task, inheriting from the core Task which is a Pydantic model."""

    # Contesto runtime
    dag_id: Optional[str] = None
    execution_id: Optional[str] = None

    # 🆕 Campi che devono essere letti dallo YAML
    retries: int = Field(default=0, description="Number of retries on failure")
    retry_delay: int = Field(
        default=0, description="Delay in seconds between retry attempts"
    )

    # 🔁 Politica sulle dipendenze (opzionale; default = ANY)
    dependency_policy: DependencyPolicy = Field(
        default=DependencyPolicy.ANY,
        description=(
            "How to interpret upstream tasks status when deciding if this task should run. "
            "Valid values: 'all', 'any', 'none'.\n"
            "'all' -> run only if ALL upstream tasks are COMPLETED (success).\n"
            "'any' -> run if AT LEAST ONE upstream task is COMPLETED.\n"
            "'none' -> run whenever all upstream tasks are in a terminal state "
            "(COMPLETED/FAILED/SKIPPED) — useful for cleanup/finalization tasks."
        ),
    )

    def execute_local(self):
        raise NotImplementedError("Subclasses must implement this method.")
