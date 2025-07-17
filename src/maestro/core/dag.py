
from typing import Dict, List
from collections import deque

from maestro.core.task import Task

class DAG:
    def __init__(self, dag_id: str = "default_dag"):
        self.dag_id = dag_id
        self.tasks: Dict[str, Task] = {}

    def add_task(self, task: Task):
        if task.task_id in self.tasks:
            raise ValueError(f"Task with id '{task.task_id}' already exists.")
        self.tasks[task.task_id] = task

    def validate(self):
        for task_id, task in self.tasks.items():
            for dep_id in task.dependencies:
                if dep_id not in self.tasks:
                    raise ValueError(f"Task '{task_id}' has a missing dependency: '{dep_id}'")
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
