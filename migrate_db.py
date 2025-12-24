import os
import shutil
import sqlite3
from datetime import datetime

DB_PATH = os.environ.get("MAESTRO_DB", "maestro.db")

def backup_db(db_path: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.backup_{ts}"
    shutil.copy2(db_path, backup_path)
    return backup_path

def exec_many(cur, sql: str):
    cur.executescript(sql)

def main():
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB non trovato: {DB_PATH}")

    backup_path = backup_db(DB_PATH)
    print(f"[OK] Backup creato: {backup_path}")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # 1) Crea tabelle nuove (shadow)
    exec_many(cur, """
    PRAGMA foreign_keys=OFF;

    DROP TABLE IF EXISTS dags__new;
    DROP TABLE IF EXISTS executions__new;
    DROP TABLE IF EXISTS tasks__new;
    DROP TABLE IF EXISTS task_attempts__new;
    DROP TABLE IF EXISTS task_dependencies__new;
    DROP TABLE IF EXISTS logs__new;

    CREATE TABLE dags__new (
      id TEXT PRIMARY KEY,
      definition_hash TEXT NOT NULL,
      dag_filepath TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT,
      is_active INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE executions__new (
      id TEXT PRIMARY KEY,
      dag_id TEXT NOT NULL,
      run_name TEXT,
      status TEXT NOT NULL,
      fail_fast TEXT NOT NULL CHECK (fail_fast IN ('ON','OFF')),
      failed_tasks INTEGER NOT NULL DEFAULT 0,
      skipped_tasks INTEGER NOT NULL DEFAULT 0,
      started_at TEXT,
      completed_at TEXT,
      trigger_type TEXT CHECK (trigger_type IN ('manual','api','schedule')),
      triggered_by TEXT,
      thread_id TEXT,
      pid INTEGER,
      FOREIGN KEY (dag_id) REFERENCES dags__new(id)
    );

    CREATE TABLE tasks__new (
      id TEXT PRIMARY KEY,
      task_id TEXT NOT NULL,
      dag_id TEXT NOT NULL,
      execution_id TEXT NOT NULL,
      status TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      retry_count INTEGER NOT NULL DEFAULT 0,
      max_retries INTEGER NOT NULL DEFAULT 0,
      output TEXT,
      output_type TEXT CHECK (output_type IN ('text','json')),
      insertion_order INTEGER,
      is_final INTEGER NOT NULL DEFAULT 0,
      thread_id TEXT,
      FOREIGN KEY (dag_id) REFERENCES dags__new(id),
      FOREIGN KEY (execution_id) REFERENCES executions__new(id),
      UNIQUE (execution_id, task_id)
    );

    CREATE TABLE task_attempts__new (
      id TEXT PRIMARY KEY,
      task_pk TEXT NOT NULL,
      execution_id TEXT NOT NULL,
      attempt_number INTEGER NOT NULL,
      status TEXT NOT NULL,
      started_at TEXT,
      completed_at TEXT,
      error TEXT,
      pid INTEGER,
      thread_id TEXT,
      FOREIGN KEY (task_pk) REFERENCES tasks__new(id),
      FOREIGN KEY (execution_id) REFERENCES executions__new(id),
      UNIQUE (task_pk, attempt_number)
    );

    CREATE TABLE task_dependencies__new (
      id TEXT PRIMARY KEY,
      dag_id TEXT NOT NULL,
      upstream_task_id TEXT NOT NULL,
      downstream_task_id TEXT NOT NULL,
      condition TEXT,
      dependency_policy TEXT CHECK (dependency_policy IN ('all','any','none')),
      FOREIGN KEY (dag_id) REFERENCES dags__new(id)
    );

    CREATE TABLE logs__new (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dag_id TEXT NOT NULL,
      execution_id TEXT NOT NULL,
      task_pk TEXT,
      attempt_id TEXT,
      level TEXT NOT NULL CHECK (level IN ('DEBUG','INFO','WARNING','ERROR')),
      message TEXT NOT NULL,
      timestamp TEXT NOT NULL,
      thread_id TEXT,
      pid INTEGER,
      FOREIGN KEY (dag_id) REFERENCES dags__new(id),
      FOREIGN KEY (execution_id) REFERENCES executions__new(id),
      FOREIGN KEY (task_pk) REFERENCES tasks__new(id),
      FOREIGN KEY (attempt_id) REFERENCES task_attempts__new(id)
    );
    """)

    # 2) Migra dags
    # ATTENZIONE: se il tuo vecchio dags.id NON è già il nome YAML stabile,
    # per ora lo teniamo uguale (così non rompiamo i riferimenti).
    # Potrai stabilizzarlo più avanti con una migrazione di "rename dag_id".
    cur.execute("""
      INSERT INTO dags__new (id, definition_hash, dag_filepath, created_at, updated_at, is_active)
      SELECT
        id,
        COALESCE(definition, definition_hash, '') as definition_hash,
        dag_filepath,
        created_at,
        NULL as updated_at,
        1 as is_active
      FROM dags
    """)

    # 3) Migra executions
    # Valori nuovi con default sensati (puoi rifinirli dopo)
    cur.execute("""
      INSERT INTO executions__new (
        id, dag_id, run_name, status, fail_fast, failed_tasks, skipped_tasks,
        started_at, completed_at, trigger_type, triggered_by, thread_id, pid
      )
      SELECT
        id,
        dag_id,
        NULL as run_name,
        status,
        'OFF' as fail_fast,
        0 as failed_tasks,
        0 as skipped_tasks,
        started_at,
        completed_at,
        NULL as trigger_type,
        NULL as triggered_by,
        thread_id,
        pid
      FROM executions
    """)

    # 4) Migra tasks (PK UUID-like + task_id = vecchio tasks.id)
    # Converti is_final se esiste già come stringa ("True"/"False") o int
    # Nota: uso randomblob per creare un id pseudo-UUID (hex 32 char)
    cur.execute("""
      INSERT INTO tasks__new (
        id, task_id, dag_id, execution_id, status, started_at, completed_at,
        retry_count, max_retries, output, output_type, insertion_order, is_final, thread_id
      )
      SELECT
        lower(hex(randomblob(16))) as id,
        t.id as task_id,
        t.dag_id,
        t.execution_id,
        t.status,
        t.started_at,
        t.completed_at,
        COALESCE(t.n_retries, 0) as retry_count,
        0 as max_retries,
        t.output,
        CASE
          WHEN json_valid(t.output) THEN 'json'
          ELSE 'text'
        END as output_type,
        t.insertion_order,
        CASE
          WHEN t.is_final IN ('True','true','1',1) THEN 1
          ELSE 0
        END as is_final,
        t.thread_id
      FROM tasks t
    """)

    # 5) Crea task_attempts (attempt 1) per ogni task migrata
    cur.execute("""
      INSERT INTO task_attempts__new (
        id, task_pk, execution_id, attempt_number, status, started_at, completed_at, error, pid, thread_id
      )
      SELECT
        lower(hex(randomblob(16))) as id,
        tn.id as task_pk,
        tn.execution_id,
        1 as attempt_number,
        tn.status,
        tn.started_at,
        tn.completed_at,
        NULL as error,
        NULL as pid,
        tn.thread_id
      FROM tasks__new tn
    """)

    # 6) Migra logs: collega task_pk tramite (execution_id, task_id)
    # attempt_id per ora NULL (in futuro lo compilerai quando logghi per attempt)
    cur.execute("""
      INSERT INTO logs__new (
        dag_id, execution_id, task_pk, attempt_id, level, message, timestamp, thread_id, pid
      )
      SELECT
        l.dag_id,
        l.execution_id,
        tn.id as task_pk,
        NULL as attempt_id,
        CASE
          WHEN l.level IN ('DEBUG','INFO','WARNING','ERROR') THEN l.level
          ELSE 'INFO'
        END as level,
        l.message,
        l.timestamp,
        l.thread_id,
        NULL as pid
      FROM logs l
      LEFT JOIN tasks__new tn
        ON tn.execution_id = l.execution_id
       AND tn.task_id = l.task_id
    """)

    # 7) Indici (sulle tabelle nuove)
    exec_many(cur, """
    CREATE INDEX IF NOT EXISTS idx_exec_dag__new ON executions__new(dag_id);
    CREATE INDEX IF NOT EXISTS idx_exec_status__new ON executions__new(status);
    CREATE INDEX IF NOT EXISTS idx_tasks_exec__new ON tasks__new(execution_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_status__new ON tasks__new(status);
    CREATE INDEX IF NOT EXISTS idx_attempts_exec__new ON task_attempts__new(execution_id);
    CREATE INDEX IF NOT EXISTS idx_deps_dag__new ON task_dependencies__new(dag_id);
    CREATE INDEX IF NOT EXISTS idx_logs_exec__new ON logs__new(execution_id);
    CREATE INDEX IF NOT EXISTS idx_logs_task__new ON logs__new(task_pk);
    """)

    # 8) Verifiche rapide
    old_tasks = cur.execute("SELECT COUNT(*) c FROM tasks").fetchone()["c"]
    new_tasks = cur.execute("SELECT COUNT(*) c FROM tasks__new").fetchone()["c"]
    old_logs = cur.execute("SELECT COUNT(*) c FROM logs").fetchone()["c"]
    new_logs = cur.execute("SELECT COUNT(*) c FROM logs__new").fetchone()["c"]

    print(f"[CHECK] tasks old={old_tasks} new={new_tasks}")
    print(f"[CHECK] logs  old={old_logs} new={new_logs}")

    # 9) Switch: rinomina tabelle (vecchie -> __old, nuove -> finali)
    exec_many(cur, """
    ALTER TABLE dags RENAME TO dags__old;
    ALTER TABLE executions RENAME TO executions__old;
    ALTER TABLE tasks RENAME TO tasks__old;
    ALTER TABLE logs RENAME TO logs__old;

    ALTER TABLE dags__new RENAME TO dags;
    ALTER TABLE executions__new RENAME TO executions;
    ALTER TABLE tasks__new RENAME TO tasks;
    ALTER TABLE task_attempts__new RENAME TO task_attempts;
    ALTER TABLE task_dependencies__new RENAME TO task_dependencies;
    ALTER TABLE logs__new RENAME TO logs;

    PRAGMA foreign_keys=ON;
    """)

    con.commit()
    con.close()
    print("[OK] Migrazione completata. Tabelle vecchie preservate con suffisso __old.")

if __name__ == "__main__":
    main()
