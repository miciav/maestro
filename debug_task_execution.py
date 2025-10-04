#!/usr/bin/env python3

import tempfile
import os

from maestro.core.orchestrator import Orchestrator
from maestro.server.tasks.base import BaseTask
from maestro.core.status_manager import StatusManager


class DebugTask(BaseTask):
    message: str
    executed: bool = False

    def execute_local(self):
        print(f"[DEBUG] Executing task {self.task_id}: {self.message}")
        self.executed = True
        print(f"[DEBUG] Task {self.task_id} executed successfully")


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "debug.db")
        print(f"[DEBUG] Using database: {db_path}")
        
        # Create orchestrator
        orchestrator = Orchestrator(log_level="INFO", db_path=db_path)
        orchestrator.register_task_type("debug_task", DebugTask)
        
        # Create DAG file
        dag_content = """
dag:
  tasks:
    - task_id: debug_task1
      type: debug_task
      message: "Hello from debug task"
"""
        dag_file = os.path.join(temp_dir, "debug_dag.yaml")
        with open(dag_file, "w") as f:
            f.write(dag_content)
        
        print("[DEBUG] Loading DAG...")
        dag = orchestrator.load_dag_from_file(dag_file)
        print(f"[DEBUG] DAG loaded: {dag.dag_id}")
        print(f"[DEBUG] Tasks: {list(dag.tasks.keys())}")
        
        print("[DEBUG] Running DAG...")
        orchestrator.run_dag(dag)
        
        print(f"[DEBUG] Task executed: {dag.tasks['debug_task1'].executed}")
        print(f"[DEBUG] Task status: {dag.tasks['debug_task1'].status}")
        
        # Check database
        with StatusManager(db_path) as sm:
            status = sm.get_task_status(dag.dag_id, "debug_task1")
            print(f"[DEBUG] Database status: {status}")


if __name__ == "__main__":
    main()
