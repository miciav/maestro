from collections import deque
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union

from croniter import croniter

from maestro.shared.task import Task


class DAGStatus(str, Enum):
    """Enumeration for the status of a DAG."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class DAG:
    def __init__(
        self,
        dag_id: str = "default_dag",
        start_time: Optional[datetime] = None,
        cron_schedule: Optional[str] = None,
        fail_fast: bool = False,  # 🆕
    ):
        self.dag_id = dag_id
        self.start_time = start_time
        self.cron_schedule = cron_schedule
        self.fail_fast = fail_fast  # 🆕
        self.tasks: Dict[str, Task] = {}
        self.status: DAGStatus = DAGStatus.PENDING
        self.execution_id: Optional[str] = None

        # Validate that only one scheduling method is used
        if start_time is not None and cron_schedule is not None:
            raise ValueError("Cannot specify both start_time and cron_schedule")

        # Validate cron expression if provided
        if cron_schedule is not None:
            if not croniter.is_valid(cron_schedule):
                raise ValueError(f"Invalid cron expression: {cron_schedule}")

    def add_task(self, task: Task):
        if task.task_id in self.tasks:
            raise ValueError(f"Task with id '{task.task_id}' already exists.")
        self.tasks[task.task_id] = task

    def validate(self):
        for task_id, task in self.tasks.items():
            for dep_id in task.dependencies:
                if dep_id not in self.tasks:
                    raise ValueError(
                        f"Task '{task_id}' has a missing dependency: '{dep_id}'"
                    )
        # Check for cycles
        self.get_execution_order()

    def get_execution_order(self) -> List[str]:
        in_degree = {task_id: 0 for task_id in self.tasks}
        adj = {task_id: [] for task_id in self.tasks}

        for task_id, task in self.tasks.items():
            for dep_id in task.dependencies:
                in_degree[task_id] += 1
                adj[dep_id].append(task_id)

        queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
        execution_order = []

        while queue:
            current_task_id = queue.popleft()
            execution_order.append(current_task_id)

            for neighbor_id in adj[current_task_id]:
                in_degree[neighbor_id] -= 1
                if in_degree[neighbor_id] == 0:
                    queue.append(neighbor_id)

        if len(execution_order) != len(self.tasks):
            raise ValueError("DAG has a cycle.")

        return execution_order

    def is_ready_to_start(self, current_time: Optional[datetime] = None) -> bool:
        """Check if the DAG is ready to start based on its scheduling configuration."""
        if current_time is None:
            current_time = datetime.now()

        if self.start_time is not None:
            # Fixed start time
            return current_time >= self.start_time
        elif self.cron_schedule is not None:
            # Cron schedule - check if we're within the current minute of a scheduled time
            cron = croniter(self.cron_schedule, current_time)
            prev_run = cron.get_prev(datetime)
            next_run = cron.get_next(datetime)

            # Consider the DAG ready if we're within 60 seconds of the scheduled time
            time_since_prev = (current_time - prev_run).total_seconds()
            return time_since_prev <= 60
        else:
            # No scheduling constraints
            return True

    def time_until_start(
        self, current_time: Optional[datetime] = None
    ) -> Optional[float]:
        """Return the number of seconds until the DAG can start, or None if no scheduling is set."""
        if current_time is None:
            current_time = datetime.now()

        if self.start_time is not None:
            # Fixed start time
            time_diff = (self.start_time - current_time).total_seconds()
            return max(0, time_diff)  # Return 0 if start_time is in the past
        elif self.cron_schedule is not None:
            # Cron schedule - get next scheduled time
            cron = croniter(self.cron_schedule, current_time)
            next_run = cron.get_next(datetime)
            time_diff = (next_run - current_time).total_seconds()
            return max(0, time_diff)
        else:
            # No scheduling constraints
            return None

    def get_next_run_time(
        self, current_time: Optional[datetime] = None
    ) -> Optional[datetime]:
        """Get the next time this DAG is scheduled to run."""
        if current_time is None:
            current_time = datetime.now()

        if self.start_time is not None:
            # Fixed start time - only runs once
            return self.start_time if current_time < self.start_time else None
        elif self.cron_schedule is not None:
            # Cron schedule - get next scheduled time
            cron = croniter(self.cron_schedule, current_time)
            return cron.get_next(datetime)
        else:
            # No scheduling constraints
            return None

    def get_schedule_description(self) -> str:
        """Get a human-readable description of the DAG's schedule."""
        if self.start_time is not None:
            return f"One-time execution at {self.start_time}"
        elif self.cron_schedule is not None:
            return f"Cron schedule: {self.cron_schedule}"
        else:
            return "No schedule (runs immediately)"

    def to_dict(self):
        """Returns a dictionary representation of the DAG."""
        return {
            "dag_id": self.dag_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "cron_schedule": self.cron_schedule,
            "fail_fast": self.fail_fast,
            "tasks": {task_id: task.to_dict() for task_id, task in self.tasks.items()},
        }

    def cron_schedule_to_aps_kwargs(self) -> Dict[str, str]:
        """Converts a cron schedule string to a dictionary of arguments for APScheduler."""
        if not self.cron_schedule:
            return {}

        parts = self.cron_schedule.split()
        if len(parts) != 5:
            raise ValueError("Invalid cron schedule format. Expected 5 parts.")

        return {
            "minute": parts[0],
            "hour": parts[1],
            "day": parts[2],
            "month": parts[3],
            "day_of_week": parts[4],
        }

    def get_explicit_exit_tasks(self) -> List[str]:
        """Return task_ids explicitly marked as final (is_final=True)."""
        return [
            task_id
            for task_id, task in self.tasks.items()
            if getattr(task, "is_final", False)
        ]

    def _build_downstream_map(self) -> Dict[str, List[str]]:
        downstream = {task_id: [] for task_id in self.tasks}
        for task in self.tasks.values():
            for dep in task.dependencies:
                downstream[dep].append(task.task_id)
        return downstream

    def get_exit_task_candidates(self) -> List[str]:
        downstream = self._build_downstream_map()
        return [tid for tid, children in downstream.items() if not children]

    def get_exit_tasks(
        self,
        status_manager,
        execution_id: str,
    ) -> List[str]:

        # 1️⃣ explicit is_final=True
        explicit = [
            t.task_id for t in self.tasks.values() if getattr(t, "is_final", False)
        ]
        if explicit:
            return explicit

        # 2️⃣ fallback: no downstream
        candidates = self.get_exit_task_candidates()

        scored = []

        for task_id in candidates:
            task = self.tasks[task_id]
            started_at = status_manager.get_task_started_at(
                self.dag_id, task_id, execution_id
            )

            if started_at:
                scored.append((started_at, task_id))
                continue

            # ⚠️ Edge case: mai partita → risali upstream
            created_at = status_manager.get_nearest_upstream_timestamp(
                self.dag_id, task_id, execution_id
            )

            if created_at:
                scored.append((created_at, task_id))

        if not scored:
            return []

        # ordina per timestamp decrescente
        scored.sort(reverse=True)

        top_time = scored[0][0]

        # ⚠️ Edge case 2: stesso timestamp → più exit-task
        return [tid for ts, tid in scored if ts == top_time]

    def evaluate_final_status(
        self,
        status_manager,
        execution_id: str,
    ) -> DAGStatus:

        exit_tasks = self.get_exit_tasks(status_manager, execution_id)

        if not exit_tasks:
            return DAGStatus.FAILED  # DAG senza uscita = errore logico

        statuses = [
            status_manager.get_task_status(self.dag_id, tid, execution_id)
            for tid in exit_tasks
        ]

        # Caso 2 — fallisce SOLO se TUTTE falliscono
        if all(status == "failed" for status in statuses):
            return DAGStatus.FAILED

        return DAGStatus.COMPLETED
