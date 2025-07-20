import sqlite3
import threading
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
import re
import random
import string
import json

# Docker-like name generator - lists of adjectives and nouns
DOCKER_ADJECTIVES = [
    "amazing", "awesome", "blissful", "bold", "brave", "charming", "clever", "cool", "dazzling", "determined",
    "eager", "ecstatic", "elegant", "epic", "exciting", "fantastic", "friendly", "gallant", "gentle", "gracious",
    "happy", "hardcore", "inspiring", "jolly", "keen", "kind", "laughing", "loving", "lucid", "magical",
    "modest", "naughty", "nervous", "nice", "objective", "optimistic", "peaceful", "pedantic", "pensive", "practical",
    "quirky", "relaxed", "romantic", "serene", "sharp", "stoic", "sweet", "tender", "thirsty", "trusting",
    "unruffled", "upbeat", "vibrant", "vigilant", "wonderful", "xenial", "youthful", "zealous", "zen"
]

DOCKER_NOUNS = [
    "albattani", "allen", "almeida", "antonelli", "archimedes", "ardinghelli", "aryabhata", "austin", "babbage", "banach",
    "banzai", "bardeen", "bartik", "bassi", "beaver", "bell", "benz", "bhabha", "bhaskara", "black",
    "blackburn", "blackwell", "bohr", "booth", "borg", "bose", "bouman", "boyd", "brahmagupta", "brattain",
    "brown", "buck", "burnell", "cannon", "carson", "cartwright", "cerf", "chandrasekhar", "chaplygin", "chatelet",
    "chatterjee", "chebyshev", "cohen", "chaum", "clarke", "colden", "cori", "cray", "curran", "curie",
    "darwin", "davinci", "dewdney", "dhawan", "diffie", "dijkstra", "dirac", "driscoll", "dubinsky", "easley",
    "edison", "einstein", "elbakyan", "elgamal", "elion", "ellis", "engelbart", "euclid", "euler", "faraday",
    "feistel", "fermat", "fermi", "feynman", "franklin", "gagarin", "galileo", "galois", "ganguly", "gates",
    "gauss", "germain", "goldberg", "goldstine", "goldwasser", "golick", "goodall", "gould", "greider", "grothendieck",
    "haibt", "hamilton", "haslett", "hawking", "heisenberg", "hermann", "herschel", "hertz", "heyrovsky", "hodgkin",
    "hofstadter", "hoover", "hopper", "hugle", "hypatia", "ishizaka", "jackson", "jang", "jemison", "jennings",
    "jepsen", "johnson", "joliot", "jones", "kalam", "kapitsa", "kare", "keldysh", "keller", "kepler",
    "khorana", "kilby", "kirch", "knuth", "kowalevski", "lalande", "lamarr", "lamport", "leakey", "leavitt",
    "lederberg", "lehmann", "lewin", "lichterman", "liskov", "lovelace", "lumiere", "mahavira", "margulis", "matsumoto",
    "maxwell", "mayer", "mccarthy", "mcclintock", "mclaren", "mclean", "mcnulty", "mendel", "mendeleev", "menshov",
    "merkle", "mestorf", "mirzakhani", "moore", "morse", "murdoch", "moser", "napier", "nash", "neumann",
    "newton", "nightingale", "nobel", "noether", "northcutt", "noyce", "panini", "pare", "pascal", "pasteur",
    "payne", "perlman", "pike", "poincare", "poitras", "proskuriakova", "ptolemy", "raman", "ramanujan", "ride",
    "montalcini", "ritchie", "robinson", "roentgen", "rosalind", "rubin", "saha", "sammet", "sanderson", "shannon",
    "shaw", "shirley", "shockley", "shtern", "sinoussi", "snyder", "solomon", "spence", "stallman", "stonebraker",
    "sutherland", "swanson", "swartz", "swirles", "taussig", "tereshkova", "tesla", "tharp", "thompson", "torvalds",
    "tu", "turing", "varahamihira", "vaughan", "visvesvaraya", "volhard", "wescoff", "wilbur", "wiles", "williams",
    "williamson", "wilson", "wing", "wozniak", "wright", "wu", "yalow", "yonath", "zhukovsky"
]


class StatusManager:
    _tables_initialized = {}  # Class-level cache to track initialized databases
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self._initialize_tables_once()

    def _initialize_tables_once(self):
        """Initialize tables only once per database file."""
        if self.db_path not in StatusManager._tables_initialized:
            with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
                conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
                conn.execute("PRAGMA foreign_keys = ON") # Enable foreign key support
                self._create_tables_in_connection(conn)
                self._migrate_db_if_needed(conn)
            StatusManager._tables_initialized[self.db_path] = True

    def _migrate_db_if_needed(self, conn):
        """Check for and apply database migrations."""
        cursor = conn.execute("PRAGMA table_info(tasks)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'insertion_order' not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN insertion_order INTEGER")
            conn.commit()

    def __enter__(self):
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
        self._conn.execute("PRAGMA foreign_keys = ON") # Enable foreign key support
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.close()

    def _create_tables_in_connection(self, conn):
        """Create tables in the provided connection."""
        # Main table for DAGs
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dags (
                id TEXT PRIMARY KEY,
                definition TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Main table for executions (must be created before tasks)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT,
                dag_id TEXT,
                status TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                thread_id TEXT,
                pid INTEGER,
                PRIMARY KEY (id, dag_id),
                FOREIGN KEY (dag_id) REFERENCES dags(id) ON DELETE CASCADE
            )
        """)

        # Main table for tasks (references executions table)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT,
                dag_id TEXT,
                execution_id TEXT,
                status TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                thread_id TEXT,
                insertion_order INTEGER,
                PRIMARY KEY (id, dag_id, execution_id),
                FOREIGN KEY (dag_id) REFERENCES dags(id) ON DELETE CASCADE,
                FOREIGN KEY (execution_id, dag_id) REFERENCES executions(id, dag_id) ON DELETE CASCADE
            )
        """)

        # Main table for logs
        conn.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dag_id TEXT,
                execution_id TEXT,
                task_id TEXT,
                level TEXT,
                message TEXT,
                timestamp TIMESTAMP,
                thread_id TEXT,
                FOREIGN KEY (dag_id) REFERENCES dags(id) ON DELETE CASCADE
            )
        """)

        # Commit the changes
        conn.commit()

    def _create_dag_if_not_exists(self, dag_id: str):
        with self._conn:
            self._conn.execute("INSERT OR IGNORE INTO dags (id) VALUES (?)", (dag_id,))
    
    def set_task_status(self, dag_id: str, task_id: str, status: str, execution_id: str = None):
        """Set task status with thread safety and timestamp tracking."""
        with self._lock:
            self._create_dag_if_not_exists(dag_id)
            current_time = datetime.now().isoformat()
            thread_id = threading.current_thread().ident
            
            # If execution_id is None, use a default value to ensure queries work
            if execution_id is None:
                execution_id = "default"
                # Ensure the default execution exists
                self._conn.execute("""
                    INSERT OR IGNORE INTO executions 
                    (id, dag_id, status, started_at, thread_id, pid)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (execution_id, dag_id, "running", current_time, str(thread_id), str(thread_id)))
            
            with self._conn:
                # Get insertion_order if it exists
                cursor = self._conn.execute(
                    "SELECT insertion_order FROM tasks WHERE id = ? AND dag_id = ? AND execution_id = ?",
                    (task_id, dag_id, execution_id)
                )
                row = cursor.fetchone()
                insertion_order = row[0] if row and row[0] is not None else None

                if status == "running":
                    self._conn.execute("""
                        INSERT OR REPLACE INTO tasks 
                        (id, dag_id, execution_id, status, started_at, thread_id, insertion_order)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (task_id, dag_id, execution_id, status, current_time, str(thread_id), insertion_order))
                elif status in ["completed", "failed"]:
                    cursor = self._conn.execute(
                        "SELECT started_at FROM tasks WHERE id = ? AND dag_id = ? AND execution_id = ?", 
                        (task_id, dag_id, execution_id)
                    )
                    row = cursor.fetchone()
                    started_at = row[0] if row else current_time
                    
                    self._conn.execute("""
                        INSERT OR REPLACE INTO tasks 
                        (id, dag_id, execution_id, status, started_at, completed_at, thread_id, insertion_order)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (task_id, dag_id, execution_id, status, started_at, current_time, str(thread_id), insertion_order))
                else:
                    self._conn.execute("""
                        INSERT OR REPLACE INTO tasks 
                        (id, dag_id, execution_id, status, thread_id, insertion_order)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (task_id, dag_id, execution_id, status, str(thread_id), insertion_order))
                
                # Explicitly commit the transaction
                self._conn.commit()

    def get_task_status(self, dag_id: str, task_id: str, execution_id: str = None) -> Optional[str]:
        """Get task status with thread safety."""
        with self._lock:
            with self._conn:
                if execution_id:
                    cursor = self._conn.execute("SELECT status FROM tasks WHERE dag_id = ? AND id = ? AND execution_id = ?", (dag_id, task_id, execution_id))
                    row = cursor.fetchone()
                    if row:
                        return row[0]
                    # If not found with specific execution_id, try the default execution
                    cursor = self._conn.execute("SELECT status FROM tasks WHERE dag_id = ? AND id = ? AND execution_id = ?", (dag_id, task_id, "default"))
                    row = cursor.fetchone()
                    return row[0] if row else None
                else:
                    # When execution_id is None, look for the default execution_id
                    cursor = self._conn.execute("SELECT status FROM tasks WHERE dag_id = ? AND id = ? AND execution_id = ?", (dag_id, task_id, "default"))
                    row = cursor.fetchone()
                    return row[0] if row else None

    def get_dag_status(self, dag_id: str, execution_id: str = None) -> Dict[str, str]:
        """Get DAG status with thread safety."""
        with self._lock:
            with self._conn:
                if execution_id:
                    cursor = self._conn.execute("SELECT id, status FROM tasks WHERE dag_id = ? AND execution_id = ?", (dag_id, execution_id))
                else:
                    cursor = self._conn.execute("SELECT id, status FROM tasks WHERE dag_id = ?", (dag_id,))
                return {row[0]: row[1] for row in cursor.fetchall()}

    def reset_dag_status(self, dag_id: str, execution_id: str = None):
        """Reset DAG status with thread safety."""
        with self._lock:
            with self._conn:
                if execution_id:
                    # Only delete tasks for this execution, not the execution itself
                    self._conn.execute("DELETE FROM tasks WHERE dag_id = ? AND execution_id = ?", (dag_id, execution_id))
                else:
                    self._conn.execute("DELETE FROM tasks WHERE dag_id = ?", (dag_id,))
                    self._conn.execute("DELETE FROM executions WHERE dag_id = ?", (dag_id,))

    def create_dag_execution(self, dag_id: str, execution_id: str) -> str:
        """Create a new DAG execution record."""
        with self._lock:
            self._create_dag_if_not_exists(dag_id)
            current_time = datetime.now().isoformat()
            thread_id = threading.current_thread().ident
            pid = threading.current_thread().ident
            
            with self._conn:
                self._conn.execute("""
                    INSERT OR REPLACE INTO executions 
                    (id, dag_id, status, started_at, thread_id, pid)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (execution_id, dag_id, "running", current_time, str(thread_id), pid))
                self._conn.commit()
            
            return execution_id
    
    def initialize_tasks_for_execution(self, dag_id: str, execution_id: str, task_ids: List[str]):
        """Initialize all tasks for a DAG execution with pending status."""
        with self._lock:
            thread_id = threading.current_thread().ident
            
            with self._conn:
                for i, task_id in enumerate(task_ids):
                    self._conn.execute("""
                        INSERT OR IGNORE INTO tasks 
                        (id, dag_id, execution_id, status, thread_id, insertion_order)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (task_id, dag_id, execution_id, "pending", str(thread_id), i))

    def update_dag_execution_status(self, dag_id: str,
                                    execution_id: str,
                                    status: str):
        """Update DAG execution status."""
        with self._lock:
            current_time = datetime.now().isoformat()
            
            with self._conn:
                if status in ["completed", "failed", "cancelled"]:
                    self._conn.execute("""
                        UPDATE executions 
                        SET status = ?, completed_at = ?
                        WHERE dag_id = ? AND id = ?
                    """, (status, current_time, dag_id, execution_id))
                else:
                    self._conn.execute("""
                        UPDATE executions 
                        SET status = ?
                        WHERE dag_id = ? AND id = ?
                    """, (status, dag_id, execution_id))
    
    def mark_incomplete_tasks_as_failed(self, dag_id: str, execution_id: str):
        """Mark all running or pending tasks as failed for a given execution."""
        with self._lock:
            current_time = datetime.now().isoformat()
            thread_id = threading.current_thread().ident
            
            with self._conn:
                # Update all running/pending tasks to failed
                self._conn.execute("""
                    UPDATE tasks 
                    SET status = 'failed', completed_at = ?, thread_id = ?
                    WHERE dag_id = ? AND execution_id = ? AND status IN ('running', 'pending')
                """, (current_time, str(thread_id), dag_id, execution_id))

    def get_running_dags(self) -> List[Dict[str, Any]]:
        """Get all currently running DAGs."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("""
                    SELECT dag_id, id, started_at, thread_id, pid
                    FROM executions 
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
                    SELECT dag_id, id, started_at, completed_at, thread_id, pid
                    FROM executions 
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
                # Get all DAGs with their latest execution info
                cursor = self._conn.execute("""
                    SELECT 
                        d.id as dag_id,
                        e.id as execution_id,
                        e.status,
                        e.started_at,
                        e.completed_at,
                        e.thread_id,
                        e.pid
                    FROM dags d
                    LEFT JOIN (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY dag_id ORDER BY started_at DESC) as rn
                        FROM executions
                    ) e ON d.id = e.dag_id AND e.rn = 1
                    ORDER BY COALESCE(e.started_at, d.created_at) DESC
                """)
                return [{
                    "dag_id": row[0],
                    "execution_id": row[1],
                    "status": row[2] if row[2] else "created",
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
                    FROM executions 
                    GROUP BY status
                """)
                status_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                cursor = self._conn.execute("SELECT COUNT(*) FROM executions")
                total_count = cursor.fetchone()[0]
                
                cursor = self._conn.execute("SELECT COUNT(DISTINCT dag_id) FROM executions")
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
                    SELECT id, status, started_at, completed_at, thread_id, pid
                    FROM executions 
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

    def cleanup_old_executions(self, days_to_keep: int = 30):
        """Clean up old execution records older than specified days."""
        with self._lock:
            with self._conn:
                cutoff_date = datetime.now() - timedelta(days=days_to_keep)
                cutoff_iso = cutoff_date.isoformat()
                
                cursor = self._conn.execute("""
                    SELECT id FROM executions 
                    WHERE started_at < ?
                """, (cutoff_iso,))
                
                executions_to_delete = [row[0] for row in cursor.fetchall()]

                if not executions_to_delete:
                    return 0

                placeholders = ",".join("?" * len(executions_to_delete))

                self._conn.execute(f"DELETE FROM logs WHERE execution_id IN ({placeholders})", executions_to_delete)
                self._conn.execute(f"DELETE FROM tasks WHERE execution_id IN ({placeholders})", executions_to_delete)
                self._conn.execute(f"DELETE FROM executions WHERE id IN ({placeholders})", executions_to_delete)

                # Optional: Clean up DAGs that have no executions left
                self._conn.execute("""
                    DELETE FROM dags 
                    WHERE id NOT IN (SELECT DISTINCT dag_id FROM executions)
                """)
                
                return len(executions_to_delete)

    def cancel_dag_execution(self, dag_id: str, execution_id: str = None) -> bool:
        """Cancel a running DAG execution."""
        with self._lock:
            with self._conn:
                if execution_id:
                    cursor = self._conn.execute("""
                        UPDATE executions 
                        SET status = 'cancelled', completed_at = ?
                        WHERE dag_id = ? AND id = ? AND status = 'running'
                    """, (datetime.now().isoformat(), dag_id, execution_id))
                else:
                    cursor = self._conn.execute("""
                        UPDATE executions 
                        SET status = 'cancelled', completed_at = ?
                        WHERE dag_id = ? AND status = 'running'
                    """, (datetime.now().isoformat(), dag_id))
                
                return cursor.rowcount > 0

    def get_dag_execution_details(self, dag_id: str, execution_id: str = None) -> Dict[str, Any]:
        """Get detailed information about a DAG execution."""
        with self._lock:
            with self._conn:
                if execution_id:
                    cursor = self._conn.execute("""
                        SELECT id, status, started_at, completed_at, thread_id, pid
                        FROM executions 
                        WHERE dag_id = ? AND id = ?
                    """, (dag_id, execution_id))
                else:
                    cursor = self._conn.execute("""
                        SELECT id, status, started_at, completed_at, thread_id, pid
                        FROM executions 
                        WHERE dag_id = ?
                        ORDER BY started_at DESC
                        LIMIT 1
                    """, (dag_id,))
                
                exec_row = cursor.fetchone()
                if not exec_row:
                    return {}
                
                execution_id = exec_row[0]
                
                task_cursor = self._conn.execute("""
                    SELECT id, status, started_at, completed_at, thread_id
                    FROM tasks 
                    WHERE dag_id = ? AND execution_id = ?
                    ORDER BY insertion_order
                """, (dag_id, execution_id))
                
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
                    INSERT INTO logs 
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
                        FROM logs 
                        WHERE dag_id = ? AND execution_id = ?
                        ORDER BY timestamp DESC
                        LIMIT ?
                    """, (dag_id, execution_id, limit))
                else:
                    cursor = self._conn.execute("""
                        SELECT task_id, level, message, timestamp, thread_id
                        FROM logs 
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

    def validate_dag_id(self, dag_id: str) -> bool:
        """Validate DAG ID format - must be alphanumeric with underscores and hyphens."""
        if not dag_id:
            return False
        # Allow alphanumeric characters, underscores, and hyphens
        return bool(re.match(r'^[a-zA-Z0-9_-]+$', dag_id))

    def check_dag_id_uniqueness(self, dag_id: str) -> bool:
        """Check if DAG ID is unique by looking at existing DAGs in the database."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("SELECT id FROM dags WHERE id = ?", (dag_id,))
                return cursor.fetchone() is None

    def generate_unique_dag_id(self) -> str:
        """Generate a unique DAG ID using Docker-like naming, ensuring uniqueness."""
        max_attempts = 100
        for _ in range(max_attempts):
            dag_id = f"{random.choice(DOCKER_ADJECTIVES)}_{random.choice(DOCKER_NOUNS)}"
            if self.check_dag_id_uniqueness(dag_id):
                return dag_id
        
        # If we can't generate a unique name after max_attempts, add a random suffix
        base_name = f"{random.choice(DOCKER_ADJECTIVES)}_{random.choice(DOCKER_NOUNS)}"
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"{base_name}_{suffix}"

    def save_dag_definition(self, dag):
        """Save the DAG definition to the database."""
        with self._lock:
            with self._conn:
                self._conn.execute("""
                    INSERT OR REPLACE INTO dags (id, definition)
                    VALUES (?, ?)
                """, (dag.dag_id, json.dumps(dag.to_dict())))

    def get_dag_definition(self, dag_id: str) -> Optional[Dict]:
        """Get the DAG definition from the database."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("SELECT definition FROM dags WHERE id = ?", (dag_id,))
                row = cursor.fetchone()
                if row and row[0]:
                    return json.loads(row[0])
                return None

    def delete_dag(self, dag_id: str) -> int:
        """Delete a DAG and all its associated executions, tasks, and logs."""
        with self._lock:
            with self._conn:
                cursor = self._conn.execute("DELETE FROM dags WHERE id = ?", (dag_id,))
                return cursor.rowcount