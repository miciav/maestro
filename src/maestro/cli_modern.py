import os
import typer
from rich.console import Console
from rich.prompt import Confirm

from maestro.core.orchestrator import Orchestrator
from maestro.core.status_manager import StatusManager


def main(
    dag_file: str = typer.Argument(..., help="Path to the DAG YAML file."),
    log_level: str = typer.Option("INFO", help="Logging level (e.g., DEBUG, INFO, WARNING)."),
    db_path: str = typer.Option("maestro.db", help="Path to the SQLite database file."),
    fail_fast: bool = typer.Option(True, help="Stop execution on first task failure."),
):
    """
    Modern CLI for Maestro to run and visualize DAGs.
    """
    console = Console()
    orchestrator = Orchestrator(log_level=log_level, db_path=db_path)

    try:
        dag = orchestrator.load_dag_from_file(dag_file)
        dag_id = os.path.splitext(os.path.basename(dag_file))[0]
        dag.dag_id = dag_id
    except Exception as e:
        console.print(f"[bold red]Error loading DAG: {e}[/bold red]")
        raise typer.Exit(code=1)

    with StatusManager(db_path) as sm:
        dag_status = sm.get_dag_status(dag_id)

    resume = False
    if dag_status and any(status != "completed" for status in dag_status.values()):
        console.print(f"[bold yellow]Partially completed DAG '{dag_id}' found.[/bold yellow]")
        resume = Confirm.ask("Do you want to resume the execution?")

        if not resume:
            console.print("[bold]Starting execution from scratch.[/bold]")

    try:
        orchestrator.run_dag(dag, resume=resume, fail_fast=fail_fast)
        console.print("[bold green]DAG execution completed successfully.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]DAG execution failed: {e}[/bold red]")
        raise typer.Exit(code=1)
    finally:
        console.print("[bold blue]Final DAG status:[/bold blue]")
        orchestrator.visualize_dag(dag)


if __name__ == "__main__":
    typer.run(main)