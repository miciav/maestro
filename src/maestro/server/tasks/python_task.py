from typing import Optional
from pydantic import Field
import io
import contextlib
import threading
import sys

from maestro.server.tasks.base import BaseTask
from maestro.server.internals.status_manager import StatusManager


class PythonTask(BaseTask):
    """
    Executes inline Python code or a .py script.
    Captures stdout/stderr (print statements) and writes them into logs table
    with proper dag_id, execution_id, and formatted message.
    """

    code: Optional[str] = Field(default=None, description="Inline Python code to execute")
    script_path: Optional[str] = Field(default=None, description="Path to a .py file to execute")

    def execute_local(self):
        """
        Esegue il codice Python catturando stdout/stderr riga per riga
        e scrivendolo DIRETTAMENTE nella tabella logs tramite StatusManager,
        oltre che sul terminale del server.
        """

        sm = StatusManager.get_instance()

        dag_id = getattr(self, "dag_id", None)
        execution_id = getattr(self, "execution_id", None)
        task_id = self.task_id

        # Se per qualche motivo non abbiamo contesto, evitiamo di rompere tutto
        if not dag_id or not execution_id:
            dag_id = dag_id or "unknown_dag"
            execution_id = execution_id or "unknown_execution"

        original_stdout = sys.stdout
        original_stderr = sys.stderr

        class _LogStream(io.TextIOBase):
            def __init__(self, orig_stream, level: str):
                self._orig = orig_stream
                self._buffer = ""
                self._level = level
                self._lock = threading.Lock()

            def write(self, s: str) -> int:
                # Scrivi comunque sul terminale del server
                self._orig.write(s)
                self._orig.flush()

                # Accumula nel buffer e spezza per newline
                with self._lock:
                    self._buffer += s
                    while "\n" in self._buffer:
                        line, self._buffer = self._buffer.split("\n", 1)
                        clean = line.rstrip("\r")
                        if clean.strip():
                            # Scrivi nel DB, una riga = un log
                            sm.add_log(
                                dag_id=dag_id,
                                execution_id=execution_id,
                                task_id=task_id,
                                message=f"[PythonTask] {clean}",
                                level=self._level
                            )
                return len(s)

            def flush(self) -> None:
                self._orig.flush()

        stdout_stream = _LogStream(original_stdout, level="INFO")
        stderr_stream = _LogStream(original_stderr, level="ERROR")

        try:
            with contextlib.redirect_stdout(stdout_stream), contextlib.redirect_stderr(stderr_stream):
                if self.code:
                    exec(self.code, {})
                elif self.script_path:
                    with open(self.script_path, "r") as f:
                        code = f.read()
                    exec(code, {})
                else:
                    raise ValueError("PythonTask requires either 'code' or 'script_path'.")
        finally:
            # Ripristina gli stream originali
            sys.stdout = original_stdout
            sys.stderr = original_stderr

