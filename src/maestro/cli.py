import typer
from rich import print

from maestro.core.orchestrator import Orchestrator

app = typer.Typer()

@app.command()
def run(dag_file: str):
    """Run a DAG from a YAML file."""
    orchestrator = Orchestrator()
    try:
        dag = orchestrator.load_dag_from_file(dag_file)
        orchestrator.run_dag(dag)
        print("[bold green]DAG execution finished.[/bold green]")
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