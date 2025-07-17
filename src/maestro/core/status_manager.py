import sqlite3
from typing import Dict, Optional

class StatusManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None

    def __enter__(self):
        self._conn = sqlite3.connect(self.db_path)
        self._create_table()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.close()

    def _create_table(self):
        with self._conn:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS task_status (
                    dag_id TEXT,
                    task_id TEXT,
                    status TEXT,
                    PRIMARY KEY (dag_id, task_id)
                )
            """)

    def set_task_status(self, dag_id: str, task_id: str, status: str):
        with self._conn:
            self._conn.execute("""
                INSERT OR REPLACE INTO task_status (dag_id, task_id, status)
                VALUES (?, ?, ?)
            """, (dag_id, task_id, status))

    def get_task_status(self, dag_id: str, task_id: str) -> Optional[str]:
        with self._conn:
            cursor = self._conn.execute("SELECT status FROM task_status WHERE dag_id = ? AND task_id = ?", (dag_id, task_id))
            row = cursor.fetchone()
            return row[0] if row else None

    def get_dag_status(self, dag_id: str) -> Dict[str, str]:
        with self._conn:
            cursor = self._conn.execute("SELECT task_id, status FROM task_status WHERE dag_id = ?", (dag_id,))
            return {row[0]: row[1] for row in cursor.fetchall()}

    def reset_dag_status(self, dag_id: str):
        with self._conn:
            self._conn.execute("DELETE FROM task_status WHERE dag_id = ?", (dag_id,))
