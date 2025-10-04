# Open Issues Identified During Codebase Review

## 1. Scheduler setup is incomplete in `Orchestrator`
* `Orchestrator.__init__` falls back to calling `StatusManager()` without arguments, but the manager requires a database path. Constructing an orchestrator without a `status_manager` or `db_path` will raise a `TypeError` before the service can start. 【F:src/maestro/server/internals/orchestrator.py†L56-L72】【F:src/maestro/server/internals/status_manager.py†L53-L69】
* Methods such as `schedule_dag` expect a `self.scheduler` attribute, yet the orchestrator never instantiates a `BackgroundScheduler`. Any attempt to schedule a DAG will crash with an `AttributeError`. 【F:src/maestro/server/internals/orchestrator.py†L112-L120】

## 2. Logging hooks target the wrong logger names
`Orchestrator.run_dag` installs database log handlers on logger names under the `maestro.core.executors` namespace, but the executors live under `maestro.server.internals.executors`. As a result, task logs never reach the database-backed handler. 【F:src/maestro/server/internals/orchestrator.py†L255-L293】【F:src/maestro/server/internals/executors/factory.py†L1-L23】

## 3. Missing DAG file path accessors in `StatusManager`
* The API and orchestrator rely on `StatusManager.get_dag_filepath`, but the method is not implemented anywhere in the class, leading to runtime errors when starting or scheduling stored DAGs. 【F:src/maestro/server/app.py†L209-L219】【F:src/maestro/server/internals/orchestrator.py†L134-L149】【F:src/maestro/server/internals/status_manager.py†L358-L387】
* Storing the DAG definition through `save_dag_definition` does not persist the originating file path, so even if the missing accessor were added there would be no value to return.

## 4. Incorrect signature usage for `save_dag_definition`
`create_dag_definition` passes the DAG file path into `StatusManager.save_dag_definition`, but the method only accepts the DAG instance. This raises a `TypeError` for every call to the `/dags` endpoint. The implementation likely needs to accept and persist the path for later retrieval. 【F:src/maestro/server/app.py†L179-L198】【F:src/maestro/server/internals/status_manager.py†L358-L365】

## 5. Test suite cannot run with the documented defaults
Running `pytest` fails before collection because required third-party dependencies (`requests`, `responses`, `typer`, `fastapi`, etc.) are missing and the `maestro` package itself is not importable until it is installed. Documented setup instructions should ensure the extras in `pyproject.toml` are installed (for example `pip install -e .[test]`) before invoking the tests. 【ace130†L1-L73】【F:pyproject.toml†L26-L62】
