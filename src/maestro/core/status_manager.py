import sqlite3
import threading
import time
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
from contextlib import contextmanager

class StatusManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None
        self._lock = threading.RLock()  # Reentrant lock for thread safety

    def __enter__(self):
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
        self._create_tables()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.close()

    def _create_tables(self):
        with self._lock:
            with self._conn:
                # Create new enhanced task status table with timestamps
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS task_status (
                        dag_id TEXT,
                        task_id TEXT,
                        status TEXT,
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        thread_id TEXT,
                        PRIMARY KEY (dag_id, task_id)
                    )
                """)
                
                # DAG execution tracking table
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS dag_executions (
                        dag_id TEXT,
                        execution_id TEXT,
                        status TEXT,
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        thread_id TEXT,
                        pid INTEGER,
                        PRIMARY KEY (dag_id, execution_id)
                    )
                """)
                
                # DAG execution logs table
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS execution_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dag_id TEXT,
                        execution_id TEXT,
                        task_id TEXT,
                        level TEXT,
                        message TEXT,
                        timestamp TIMESTAMP,
                        thread_id TEXT
                    )
                """)

    def set_task_status(self, dag_id: str, task_id: str, status: str, execution_id: str = None):
        """Set task status with thread safety and timestamp tracking."""
        with self._lock:
            current_time = datetime.now().isoformat()
            thread_id = threading.current_thread().ident
            
            with self._conn:
                if status == "running":
                    self._conn.execute("""
                        INSERT OR REPLACE INTO task_status 
                        (dag_id, task_id, status, started_at, thread_id)
                        VALUES (?, ?, ?, ?, ?)
                    """, (dag_id, task_id, status, current_time, str(thread_id)))
                elif status in ["completed", "failed"]:
                    # Get existing started_at if available
                    cursor = self._conn.execute(
                        "SELECT started_at FROM task_status WHERE dag_id = ? AND task_id = ?", 
                        (dag_id, task_id)
                    )
                    row = cursor.fetchone()
                    started_at = row[0] if row else current_time
                    
                    self._conn.execute("""
                        INSERT OR REPLACE INTO task_status 
                        (dag_id, task_id, status, started_at, completed_at, thread_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (dag_id, task_id, status, started_at, current_time, str(thread_id)))
                else:
                    self._conn.execute("""
                        INSERT OR REPLACE INTO task_status 
                        (dag_id, task_id, status, thread_id)
                        VALUES (?, ?, ?, ?)
                    """, (dag_id, task_id, status, str(thread_id)))

    def get_task_status(self, dag_id: str, task_id: str) -> Optional[str]:
        """Get task status with thread safety."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("SELECT status FROM task_status WHERE dag_id = ? AND task_id = ?", (dag_id, task_id))
                row = cursor.fetchone()
                return row[0] if row else None

    def get_dag_status(self, dag_id: str) -> Dict[str, str]:
        """Get DAG status with thread safety."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("SELECT task_id, status FROM task_status WHERE dag_id = ?", (dag_id,))
                return {row[0]: row[1] for row in cursor.fetchall()}

    def reset_dag_status(self, dag_id: str, execution_id: str = None):
        """Reset DAG status with thread safety."""
        with self._lock:
            with self._conn:
                self._conn.execute("DELETE FROM task_status WHERE dag_id = ?", (dag_id,))
                if execution_id:
                    self._conn.execute("DELETE FROM dag_executions WHERE dag_id = ? AND execution_id = ?", (dag_id, execution_id))

    def create_dag_execution(self, dag_id: str, execution_id: str) -> str:
        """Create a new DAG execution record."""
        with self._lock:
            current_time = datetime.now().isoformat()
            thread_id = threading.current_thread().ident
            pid = threading.current_thread().ident  # Using thread ident as process identifier
            
            with self._conn:
                self._conn.execute("""
                    INSERT OR REPLACE INTO dag_executions 
                    (dag_id, execution_id, status, started_at, thread_id, pid)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (dag_id, execution_id, "running", current_time, str(thread_id), pid))
            
            return execution_id

    def update_dag_execution_status(self, dag_id: str, execution_id: str, status: str):
        """Update DAG execution status."""
        with self._lock:
            current_time = datetime.now().isoformat()
            
            with self._conn:
                if status in ["completed", "failed", "cancelled"]:
                    self._conn.execute("""
                        UPDATE dag_executions 
                        SET status = ?, completed_at = ?
                        WHERE dag_id = ? AND execution_id = ?
                    """, (status, current_time, dag_id, execution_id))
                else:
                    self._conn.execute("""
                        UPDATE dag_executions 
                        SET status = ?
                        WHERE dag_id = ? AND execution_id = ?
                    """, (status, dag_id, execution_id))

    def get_running_dags(self) -> List[Dict[str, Any]]:
        """Get all currently running DAGs."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("""
                    SELECT dag_id, execution_id, started_at, thread_id, pid
                    FROM dag_executions 
                    WHERE status = 'running'
                    ORDER BY started_at DESC
                """)
                return [{
                    "dag_id": row[0],
                    "execution_id": row[1],
                    "started_at": row[2],
                    "thread_id": row[3],
                    "pid": row[4]
                } for row in cursor.fetchall()]

    def get_dags_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get all DAGs with a specific status."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("""
                    SELECT dag_id, execution_id, started_at, completed_at, thread_id, pid
                    FROM dag_executions 
                    WHERE status = ?
                    ORDER BY started_at DESC
                """, (status,))
                return [{
                    "dag_id": row[0],
                    "execution_id": row[1],
                    "started_at": row[2],
                    "completed_at": row[3],
                    "thread_id": row[4],
                    "pid": row[5]
                } for row in cursor.fetchall()]

    def get_all_dags(self) -> List[Dict[str, Any]]:
        """Get all DAGs regardless of status."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("""
                    SELECT dag_id, execution_id, status, started_at, completed_at, thread_id, pid
                    FROM dag_executions 
                    ORDER BY started_at DESC
                """)
                return [{
                    "dag_id": row[0],
                    "execution_id": row[1],
                    "status": row[2],
                    "started_at": row[3],
                    "completed_at": row[4],
                    "thread_id": row[5],
                    "pid": row[6]
                } for row in cursor.fetchall()]

    def get_dag_summary(self) -> Dict[str, Any]:
        """Get summary statistics of all DAGs."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("""
                    SELECT status, COUNT(*) as count
                    FROM dag_executions 
                    GROUP BY status
                """)
                status_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Get total count
                cursor = self._conn.execute("SELECT COUNT(*) FROM dag_executions")
                total_count = cursor.fetchone()[0]
                
                # Get unique DAGs count
                cursor = self._conn.execute("SELECT COUNT(DISTINCT dag_id) FROM dag_executions")
                unique_dags = cursor.fetchone()[0]
                
                return {
                    "total_executions": total_count,
                    "unique_dags": unique_dags,
                    "status_counts": status_counts
                }

    def get_dag_history(self, dag_id: str) -> List[Dict[str, Any]]:
        """Get execution history for a specific DAG."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("""
                    SELECT execution_id, status, started_at, completed_at, thread_id, pid
                    FROM dag_executions 
                    WHERE dag_id = ?
                    ORDER BY started_at DESC
                """, (dag_id,))
                return [{
                    "execution_id": row[0],
                    "status": row[1],
                    "started_at": row[2],
                    "completed_at": row[3],
                    "thread_id": row[4],
                    "pid": row[5]
                } for row in cursor.fetchall()]

    def cleanup_old_executions(self, days_to_keep: int = 30) -> int:
        """Clean up old execution records older than specified days."""
        with self._lock:
            with self._conn:
                # Calculate the cutoff date
                cutoff_date = datetime.now() - timedelta(days=days_to_keep)
                cutoff_iso = cutoff_date.isoformat()
                
                # Count records to be deleted
                cursor = self._conn.execute("""
                    SELECT COUNT(*) FROM dag_executions 
                    WHERE started_at < ?
                """, (cutoff_iso,))
                count_to_delete = cursor.fetchone()[0]
                
                # Delete old records
                self._conn.execute("""
                    DELETE FROM dag_executions 
                    WHERE started_at < ?
                """, (cutoff_iso,))
                
                self._conn.execute("""
                    DELETE FROM task_status 
                    WHERE dag_id NOT IN (SELECT DISTINCT dag_id FROM dag_executions)
                """)
                
                self._conn.execute("""
                    DELETE FROM execution_logs 
                    WHERE dag_id NOT IN (SELECT DISTINCT dag_id FROM dag_executions)
                """)
                
                return count_to_delete

    def cancel_dag_execution(self, dag_id: str, execution_id: str = None) -> bool:
        """Cancel a running DAG execution."""
        with self._lock:
            with self._conn:
                if execution_id:
                    # Cancel specific execution
                    cursor = self._conn.execute("""
                        UPDATE dag_executions 
                        SET status = 'cancelled', completed_at = ?
                        WHERE dag_id = ? AND execution_id = ? AND status = 'running'
                    """, (datetime.now().isoformat(), dag_id, execution_id))
                else:
                    # Cancel all running executions for this DAG
                    cursor = self._conn.execute("""
                        UPDATE dag_executions 
                        SET status = 'cancelled', completed_at = ?
                        WHERE dag_id = ? AND status = 'running'
                    """, (datetime.now().isoformat(), dag_id))
                
                return cursor.rowcount > 0

    def get_dag_execution_details(self, dag_id: str, execution_id: str = None) -> Dict[str, Any]:
        """Get detailed information about a DAG execution."""
        with self._lock:
            with self._conn:
                if execution_id:
                    # Get specific execution
                    cursor = self._conn.execute("""
                        SELECT execution_id, status, started_at, completed_at, thread_id, pid
                        FROM dag_executions 
                        WHERE dag_id = ? AND execution_id = ?
                    """, (dag_id, execution_id))
                else:
                    # Get latest execution
                    cursor = self._conn.execute("""
                        SELECT execution_id, status, started_at, completed_at, thread_id, pid
                        FROM dag_executions 
                        WHERE dag_id = ?
                        ORDER BY started_at DESC
                        LIMIT 1
                    """, (dag_id,))
                
                exec_row = cursor.fetchone()
                if not exec_row:
                    return {}
                
                execution_id = exec_row[0]
                
                # Get task statuses for this execution
                task_cursor = self._conn.execute("""
                    SELECT task_id, status, started_at, completed_at, thread_id
                    FROM task_status 
                    WHERE dag_id = ?
                    ORDER BY started_at
                """, (dag_id,))
                
                tasks = [{
                    "task_id": row[0],
                    "status": row[1],
                    "started_at": row[2],
                    "completed_at": row[3],
                    "thread_id": row[4]
                } for row in task_cursor.fetchall()]
                
                return {
                    "execution_id": execution_id,
                    "status": exec_row[1],
                    "started_at": exec_row[2],
                    "completed_at": exec_row[3],
                    "thread_id": exec_row[4],
                    "pid": exec_row[5],
                    "tasks": tasks
                }

    def log_message(self, dag_id: str, execution_id: str, task_id: str, level: str, message: str):
        """Log a message with thread identification."""
        with self._lock:
            current_time = datetime.now().isoformat()
            thread_id = threading.current_thread().ident
            
            with self._conn:
                self._conn.execute("""
                    INSERT INTO execution_logs 
                    (dag_id, execution_id, task_id, level, message, timestamp, thread_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (dag_id, execution_id, task_id, level, message, current_time, str(thread_id)))

    def get_execution_logs(self, dag_id: str, execution_id: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get execution logs for a DAG."""
        with self._lock:
            with self._conn:
                if execution_id:
                    cursor = self._conn.execute("""
                        SELECT task_id, level, message, timestamp, thread_id
                        FROM execution_logs 
                        WHERE dag_id = ? AND execution_id = ?
                        ORDER BY timestamp DESC
                        LIMIT ?
                    """, (dag_id, execution_id, limit))
                else:
                    cursor = self._conn.execute("""
                        SELECT task_id, level, message, timestamp, thread_id
                        FROM execution_logs 
                        WHERE dag_id = ?
                        ORDER BY timestamp DESC
                        LIMIT ?
                    """, (dag_id, limit))
                
                return [{
                    "task_id": row[0],
                    "level": row[1],
                    "message": row[2],
                    "timestamp": row[3],
                    "thread_id": row[4]
                } for row in cursor.fetchall()]
