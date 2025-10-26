from maestro.server.tasks.base import BaseTask

class PythonTask(BaseTask):
    """
    Task che esegue codice Python inline o da file.
    Supporta campi: 'code', 'script', 'file', 'path' o 'function'.
    """

    def __init__(self, task_id: str, code=None, script=None, file=None, path=None, function=None, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        # Accetta diversi alias per compatibilità
        self.code = code or script
        self.file = file or path
        self.function = function

    def run(self, **context):
        """Esegue codice Python dinamicamente."""
        self.log(f"[PythonTask] Esecuzione del task '{self.task_id}'")

        # Caso 1: codice inline
        if self.code:
            local_ctx = {}
            try:
                exec(self.code, {}, local_ctx)
                result = local_ctx.get("result")
                self.log(f"[PythonTask] Risultato: {result}")
                return result
            except Exception as e:
                self.log(f"[PythonTask] Errore durante exec: {e}")
                raise

        # Caso 2: codice da file
        elif self.file:
            try:
                with open(self.file, "r") as f:
                    code = f.read()
                local_ctx = {}
                exec(code, {}, local_ctx)
                result = local_ctx.get("result")
                self.log(f"[PythonTask] Risultato file: {result}")
                return result
            except Exception as e:
                self.log(f"[PythonTask] Errore durante exec file: {e}")
                raise

        # Caso 3: funzione importata
        elif self.function:
            try:
                module_name, func_name = self.function.rsplit(".", 1)
                module = __import__(module_name, fromlist=[func_name])
                func = getattr(module, func_name)
                result = func(**context)
                self.log(f"[PythonTask] Risultato funzione: {result}")
                return result
            except Exception as e:
                self.log(f"[PythonTask] Errore durante import funzione: {e}")
                raise

        else:
            self.log("[PythonTask] Nessun campo valido (code/script/file/function) fornito.")
            return None
