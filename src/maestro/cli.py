
import typer
from rich import print
from rich.live import Live
import time
import functools

from maestro.core.dag import DAG
from maestro.core.orchestrator import Orchestrator
from maestro.utils.legacy_ui import StatusManager, ProgressTracker, DisplayManager

app = typer.Typer()

@app.command()
def run(dag_file: str, 
        modern_ui: bool = typer.Option(False, "--modern-ui", 
                                       help="Enable modern UI for DAG execution.")):
    """Run a DAG from a YAML file."""
    orchestrator = Orchestrator() # Initialize orchestrator with global console
    try:
        dag: DAG = orchestrator.load_dag_from_file(dag_file)
        if modern_ui:
            import networkx as nx
            nx_dag = nx.DiGraph()
            for task_id, task in dag.tasks.items():
                nx_dag.add_node(task_id)
                for dep_id in task.dependencies:
                    nx_dag.add_edge(dep_id, task_id)

            status_manager = StatusManager(nx_dag)
            progress_tracker = ProgressTracker(len(dag.tasks))
            display_manager = DisplayManager(nx_dag, status_manager, progress_tracker)

            def status_update_callback():
                live.update(display_manager.display())

            with Live(screen=True, refresh_per_second=4) as live:
                # Run the orchestrator with modern UI in a separate thread
                execution_id = orchestrator.run_dag_in_thread(
                    dag=dag,
                    status_callback=status_update_callback
                )

                # Monitoring thread
                while True:
                    live.update(display_manager.display())
                    time.sleep(0.1) # Serve UI updates
                    with orchestrator.status_manager as sm:
                        execution_details = sm.get_dag_execution_details(dag.dag_id, execution_id)
                    if execution_details.get("status") in ("completed", "failed"):
                        break

            print(f"[bold green]DAG execution finished with status: {execution_details.get('status')}[/bold green]")
        else:
            execution_id = orchestrator.run_dag_in_thread(dag)
            print(f"[bold green]DAG execution started with ID: {execution_id}.[/bold green]")
    except Exception as e:
        print(f"[bold red]Error: {e}[/bold red]")

@app.command()
def validate(dag_file: str):
    """Validate a DAG from a YAML file."""
    orchestrator = Orchestrator()
    try:
        orchestrator.load_dag_from_file(dag_file)
        print("[bold green]DAG is valid.[/bold green]")
    except Exception as e:
        print(f"[bold red]Error: {e}[/bold red]")

@app.command()
def visualize(dag_file: str):
    """Visualize a DAG from a YAML file."""
    orchestrator = Orchestrator()
    try:
        dag = orchestrator.load_dag_from_file(dag_file)
        orchestrator.visualize_dag(dag)
    except Exception as e:
        print(f"[bold red]Error: {e}[/bold red]")

@app.command()
def status(dag_file: str):
    """Get the status of a DAG from a YAML file."""
    orchestrator = Orchestrator()
    try:
        dag = orchestrator.load_dag_from_file(dag_file)
        status = orchestrator.get_dag_status(dag)
        print(status)
    except Exception as e:
        print(f"[bold red]Error: {e}[/bold red]")

if __name__ == "__main__":
    app()
