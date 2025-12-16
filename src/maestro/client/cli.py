#!/usr/bin/env python3

import typer
import signal
import sys
import os
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from datetime import datetime
import subprocess
import time
import threading
import json

from .api_client import MaestroAPIClient

def _print_status_table(console, dag_id: str, execution_id: str, status: dict):
    """
    Helper per stampare lo stesso output di 'maestro status'.
    """
    from rich.table import Table

    table = Table(title=f"DAG Status — {dag_id} ({execution_id})")

    table.add_column("Task ID", style="magenta")
    table.add_column("Status", style="cyan")
    table.add_column("Started", style="green")
    table.add_column("Completed", style="yellow")

    for task in status.get("tasks", []):
        table.add_row(
            task["task_id"],
            task["status"],
            task.get("started_at", "") or "",
            task.get("completed_at", "") or ""
        )

    console.print(table)

app = typer.Typer(help="Maestro CLI Client - Communicate with Maestro REST API server")
console = Console()

# Global API client
api_client = MaestroAPIClient()

def check_server_connection():
    """Check if the server is running and provide helpful feedback"""
    if not api_client.is_server_running():
        console.print("[red]Error: Maestro server is not running![/red]")
        console.print()
        console.print("To start the server, run:")
        console.print("  [bold]maestro server start[/bold]")
        console.print()
        console.print("Or start it manually:")
        console.print("  [bold]python -m maestro.server.app[/bold]")
        raise typer.Exit(1)

@app.command()
def create(
    dag_file: str = typer.Argument(..., help="Path to the DAG YAML file"),
    dag_id: Optional[str] = typer.Option(None, "--dag-id", help="Optional: Specify a DAG ID"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Create a new DAG from a YAML file"""
    api_client.base_url = server_url
    check_server_connection()

    try:
        dag_file_path = os.path.abspath(dag_file)
        if not os.path.exists(dag_file_path):
            console.print(f"[red]Error: DAG file not found: {dag_file_path}[/red]")
            raise typer.Exit(1)

        response = api_client.create_dag(dag_file_path, dag_id)
        console.print(f"[bold green]✓ {response['message']}[/bold green]")
        console.print(f"[cyan]DAG ID:[/cyan] {response['dag_id']}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def run(
    dag_file: str = typer.Argument(..., help="Path to DAG YAML file"),
    detach: bool = typer.Option(False, "--detach", "-d", help="Run in detached mode"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """
    Run a DAG.
    - Default = attached mode (stream logs until completion)
    - --detach = detached mode with automatic status reporting at the end
    """

    api_client.base_url = server_url
    check_server_connection()

    console.print(f"[cyan]Creating DAG from file: {dag_file}[/cyan]")

    # 1. CREATE DAG
    try:
        result = api_client.create_dag(dag_file)
    except Exception as e:
        console.print(f"[red]Error creating DAG: {e}[/red]")
        raise typer.Exit(1)

    dag_id = result.get("dag_id")
    if not dag_id:
        console.print("[red]Server did not return dag_id[/red]")
        raise typer.Exit(1)

    console.print(f"[green]DAG created: {dag_id}[/green]")

    # 2. RUN DAG
    try:
        run_info = api_client.run_dag(dag_id)
    except Exception as e:
        console.print(f"[red]Error running DAG: {e}[/red]")
        raise typer.Exit(1)

    execution_id = run_info.get("execution_id")
    if not execution_id:
        console.print("[red]Server did not return execution_id[/red]")
        raise typer.Exit(1)

    console.print(f"[green]Execution started: {execution_id}[/green]")

    # -----------------------------------------------------------------------
    # 3A — DETACHED MODE
    # -----------------------------------------------------------------------
    if detach:

        console.print("[yellow]Running in detached mode.[/yellow]\n")

        console.print("Available commands:")
        console.print(f"  • maestro attach {dag_id} - Attach to live log stream")
        console.print(f"  • maestro stop {dag_id} or Ctrl+C - Stop execution\n")

        console.print("[dim]...running DAG...[/dim]\n")

        # POLLING FINO A COMPLETAMENTO
        while True:
            try:
                status = api_client.get_dag_status(dag_id, execution_id)
                if status["status"] in ("completed", "failed", "cancelled"):
                    break
                time.sleep(1)
            except KeyboardInterrupt:
                console.print("\n[yellow]Detached (polling stopped).[/yellow]")
                return
            except Exception:
                # se il server fosse temporaneamente non raggiungibile
                time.sleep(1)

        console.print(f"\n[green]DAG completed[/green]\n")

        console.print("Available commands:")
        console.print(f"  • maestro status {dag_id} - Check DAG status")
        console.print(f"  • maestro log {dag_id} - View logs\n")

        return

    # -----------------------------------------------------------------------
    # 3B — ATTACHED MODE
    # -----------------------------------------------------------------------

    console.print(f"[bold cyan]Attaching to live logs...[/bold cyan]")
    console.print("[dim]Press Ctrl+C to detach[/dim]\n")

    try:
        for log_entry in api_client.stream_dag_logs_v1(dag_id, execution_id):
            # Evento di fine DAG
            if log_entry.get("event") == "DAG_COMPLETED":
                console.print("\n[green]DAG completed — detaching.[/green]\n")
                break

            if "level" not in log_entry:
                continue

            level = log_entry["level"]
            ts = log_entry["timestamp"].split("T")[1].split(".")[0]
            task = log_entry["task_id"]
            msg = log_entry["message"]

            style = {
                "ERROR": "red",
                "WARNING": "yellow",
                "INFO": "green",
                "DEBUG": "blue",
            }.get(level, "white")

            console.print(f"[dim]{ts}[/dim] [{style}]{level}[/] [magenta]{task}[/]: {msg}")

    except KeyboardInterrupt:
        console.print("\n[yellow]Detached from log stream[/yellow]")
        return

    # 🔥 DOPO LO STREAMING → MOSTRA STATUS COMPLETO
    console.print("[cyan]DAG final status:[/cyan]\n")

    try:
        status = api_client.get_dag_status(dag_id, execution_id)
        _print_status_table(console, dag_id, execution_id, status)
    except Exception as e:
        console.print(f"[red]Unable to fetch DAG status: {e}[/red]")


@app.command()
def validate(
    dag_file: str = typer.Argument(..., help="Path to the DAG YAML file"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Validate a DAG file without executing it"""
    api_client.base_url = server_url
    check_server_connection()

    try:
        dag_file_path = os.path.abspath(dag_file)
        if not os.path.exists(dag_file_path):
            console.print(f"[red]Error: DAG file not found: {dag_file_path}[/red]")
            raise typer.Exit(1)

        response = api_client.validate_dag(dag_file_path)
        if response["valid"]:
            console.print(f"[bold green]✓ DAG is valid: {response['dag_id']}[/bold green]")
            console.print(f"[cyan]Total tasks:[/cyan] {response['total_tasks']}")
            if response["tasks"]:
                console.print("[bold blue]Tasks:[/bold blue]")
                for task in response["tasks"]:
                    deps = f" (dependencies: {', '.join(task['dependencies'])})" if task['dependencies'] else ""
                    console.print(f"  • {task['task_id']} ({task['type']}){deps}")
        else:
            console.print(f"[bold red]✗ DAG validation failed: {response['error']}[/bold red]")
            raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def status(
    dag_id: str = typer.Argument(..., help="DAG ID to check status"),
    execution_id: Optional[str] = typer.Option(None, "--execution-id", help="Specific execution ID"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Show status of a DAG execution"""
    api_client.base_url = server_url
    check_server_connection()
    
    try:
        response = api_client.get_dag_status(dag_id, execution_id)
        
        # Main status table
        table = Table(title=f"DAG Status: {dag_id}")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Execution ID", response["execution_id"])
        table.add_row("Status", response["status"])
        table.add_row("Started", response["started_at"] or "N/A")
        table.add_row("Completed", response["completed_at"] or "N/A")
        table.add_row("Thread ID", response["thread_id"] or "N/A")
        
        console.print(table)
        
        # Tasks table
        if response["tasks"]:
            tasks_table = Table(title="Tasks")
            tasks_table.add_column("Task ID", style="cyan")
            tasks_table.add_column("Status", style="magenta")
            tasks_table.add_column("Started", style="green")
            tasks_table.add_column("Completed", style="yellow")
            
            for task in response["tasks"]:
                status_style = {
                    "completed": "green",
                    "running": "yellow",
                    "failed": "red",
                    "pending": "blue"
                }.get(task["status"], "white")
                
                tasks_table.add_row(
                    task["task_id"],
                    f"[{status_style}]{task['status']}[/{status_style}]",
                    task.get("started_at", "N/A") or "N/A",
                    task.get("completed_at", "N/A") or "N/A"
                )
            
            console.print(tasks_table)
        
    except FileNotFoundError:
        console.print(f"[red]DAG execution not found: {dag_id}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def log(
    dag_id: str = typer.Argument(..., help="DAG ID to get logs for"),
    execution_id: Optional[str] = typer.Option(None, "--execution-id", help="Specific execution ID"),
    limit: int = typer.Option(100, "--limit", help="Number of log entries to show"),
    task_filter: Optional[str] = typer.Option(None, "--task", help="Filter logs by task ID"),
    level_filter: Optional[str] = typer.Option(None, "--level", help="Filter logs by level"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Show logs for a DAG execution"""
    api_client.base_url = server_url
    check_server_connection()

    try:

        response = api_client.get_dag_logs_v1(dag_id, execution_id, limit, task_filter, level_filter)

        # 🔧 FIX 1: se la risposta è una stringa JSON, decodificala
        if isinstance(response, str):
            import json
            try:
                response = json.loads(response)
            except Exception:
                console.print("[red]Error: invalid JSON returned from server[/red]")
                raise typer.Exit(1)

        # Se la risposta è un dizionario con chiave "logs", estrai la lista
        if isinstance(response, dict) and "logs" in response:
            response = response["logs"]

        # 🔧 FIX 2: se è una lista di stringhe JSON, deserializzale
        if isinstance(response, list) and all(isinstance(e, str) for e in response):
            try:
                response = [json.loads(e) for e in response]
            except Exception:
                console.print("[red]Error: invalid JSON structure in log entries[/red]")
                raise typer.Exit(1)

        if not response:
            console.print(f"[yellow]No logs found for DAG: {dag_id}[/yellow]")
            return

        console.print(f"[bold cyan]Logs: {dag_id}[/bold cyan]")
        if execution_id:
            console.print(f"[dim]Execution ID: {execution_id}[/dim]")
        console.print(f"[dim]Showing {len(response)} log entries[/dim]")
        console.print()

        for log_entry in reversed(response):
            level_style = {
                "ERROR": "red",
                "WARNING": "yellow",
                "INFO": "green",
                "DEBUG": "blue"
            }.get(log_entry["level"], "white")

            timestamp = log_entry["timestamp"].split("T")[1].split(".")[0] if "T" in log_entry["timestamp"] else log_entry["timestamp"]

            console.print(f"[dim]{timestamp}[/dim] [{level_style}]{log_entry['level']}[/{level_style}] [magenta]{log_entry['task_id']}[/magenta]: {log_entry['message']}")

        console.print()
        console.print(f"[dim]💡 Tip: Use 'maestro attach {dag_id}' for live log streaming[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def attach(
    dag_id: str = typer.Argument(..., help="DAG ID to attach logs for"),
    execution_id: Optional[str] = typer.Option(None, "--execution-id", help="Specific execution ID"),
    task_filter: Optional[str] = typer.Option(None, "--task", help="Filter logs by task ID"),
    level_filter: Optional[str] = typer.Option(None, "--level", help="Filter logs by level"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Attach to live log stream for a DAG execution"""
    api_client.base_url = server_url
    check_server_connection()
    
    def handle_exit(signum, frame):
        console.print("\n[yellow]Detached from log stream[/yellow]")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)
    
    console.print(f"[bold cyan]Attaching to live logs for DAG: {dag_id}[/bold cyan]")
    if execution_id:
        console.print(f"[dim]Execution ID: {execution_id}[/dim]")
    console.print(f"[dim]Press Ctrl+C to detach[/dim]\n")
    
    try:
        for log_entry in api_client.stream_dag_logs_v1(dag_id, execution_id, task_filter, level_filter):
            if "error" in log_entry:
                console.print(f"[red]Stream error: {log_entry['error']}[/red]")
                break
            
            # 👉 Evento interno del server: DAG completata → detach automatico
            if "event" in log_entry and log_entry["event"] == "DAG_COMPLETED":
                console.print("\n[green][DAG COMPLETED] Detaching...[/green]\n")
                return

            level_style = {
                "ERROR": "red",
                "WARNING": "yellow",
                "INFO": "green",
                "DEBUG": "blue"
            }.get(log_entry["level"], "white")
            
            timestamp = log_entry["timestamp"].split("T")[1].split(".")[0] if "T" in log_entry["timestamp"] else log_entry["timestamp"]
            
            console.print(f"[dim]{timestamp}[/dim] [{level_style}]{log_entry['level']}[/{level_style}] [magenta]{log_entry['task_id']}[/magenta]: {log_entry['message']}")
    
    except KeyboardInterrupt:
        console.print("\n[yellow]Detached from log stream[/yellow]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def rm(
    dag_id: Optional[str] = typer.Argument(None, help="DAG ID to remove"),
    all: bool = typer.Option(False, "--all", "-a", help="Remove all non-running DAGs (or all DAGs with --force)"),
    force: bool = typer.Option(False, "--force", "-f", help="Force removal, even for running DAGs"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Remove a DAG and its executions"""
    api_client.base_url = server_url
    check_server_connection()

    try:
        if all:
            # Get all DAGs
            dags = api_client.list_dags_v1()
            removed_count = 0
            
            for dag in dags:
                # Remove all non-running DAGs, or all if --force is used
                if force or dag['status'] not in ['running']:
                    response = api_client.remove_dag(dag['dag_id'], force)
                    console.print(f"[bold green]✓ Removed DAG: {dag['dag_id']} (status: {dag['status']})[/bold green]")
                    removed_count += 1
                else:
                    console.print(f"[yellow]⚠ Skipped running DAG: {dag['dag_id']}[/yellow]")
            
            if removed_count == 0:
                console.print("[yellow]No DAGs to remove.[/yellow]")
            else:
                console.print(f"[bold green]✓ Removed {removed_count} DAG(s).[/bold green]")
        else:
            if not dag_id:
                console.print("[red]Error: Please provide a DAG ID or use --all flag[/red]")
                raise typer.Exit(1)
            response = api_client.remove_dag(dag_id, force)
            console.print(f"[bold green]✓ {response['message']}[/bold green]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def stop(
    dag_id: str = typer.Argument(..., help="DAG ID to stop"),
    execution_id: Optional[str] = typer.Option(None, "--execution-id", help="Specific execution ID"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Stop a running DAG execution"""
    api_client.base_url = server_url
    check_server_connection()

    try:
        response = api_client.stop_dag(dag_id, execution_id)
        console.print(f"[bold green]✓ {response['message']}[/bold green]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def resume(
    dag_id: str = typer.Argument(..., help="DAG ID to resume"),
    execution_id: str = typer.Argument(..., help="Execution ID to resume"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Resume a previously stopped DAG execution"""
    api_client.base_url = server_url
    check_server_connection()

    try:
        response = api_client.resume_dag(dag_id, execution_id)
        console.print(f"[bold green]✓ {response['message']}[/bold green]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def cleanup(
    days: int = typer.Option(30, "--days", help="Days to keep (older executions will be deleted)"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """Clean up old execution records"""
    api_client.base_url = server_url
    check_server_connection()
    
    try:
        response = api_client.cleanup_old_executions(days)
        console.print(f"[green]{response['message']}[/green]")
    
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command(name="ls")
def list_dags(
    filter: Optional[str] = typer.Option(None, "--filter", "-f", help="Filter by status (active, terminated, all)"),
    server_url: str = typer.Option("http://localhost:8000", "--server", help="Maestro server URL")
):
    """List all DAGs with optional filtering"""
    api_client.base_url = server_url
    check_server_connection()
    
    try:
        response = api_client.list_dags_v1(filter)
        
        if not response:
            if filter:
                console.print(f"[yellow]No DAGs found with filter '{filter}'[/yellow]")
            else:
                console.print("[yellow]No DAGs found[/yellow]")
            return
        
        # Create table
        table = Table(title="Maestro DAGs")
        table.add_column("DAG ID", style="cyan")
        table.add_column("Execution ID", style="blue")
        table.add_column("Status", style="green")
        table.add_column("Started", style="magenta")
        table.add_column("Completed", style="yellow")
        table.add_column("Thread ID", style="dim")
        
        for dag in response:
            status_style = {
                "completed": "green",
                "running": "yellow",
                "failed": "red",
                "cancelled": "orange1"
            }.get(dag.get("status"), "white")
            
            table.add_row(
                dag["dag_id"],
                dag["execution_id"][:8] + "..." if dag["execution_id"] else "N/A",
                f"[{status_style}]{dag.get('status', 'N/A')}[/{status_style}]",
                dag.get("started_at", "N/A"),
                dag.get("completed_at", "N/A"),
                str(dag.get("thread_id", "N/A"))
            )
        
        console.print(table)
        console.print(f"\n[dim]Total DAGs: {len(response)}[/dim]")
        
        # Show helpful tips
        if not filter:
            console.print("\n[dim]💡 Tips:[/dim]")
            console.print("[dim]  • Use --filter active to show only running DAGs[/dim]")
            console.print("[dim]  • Use --filter terminated to show completed, failed, or cancelled DAGs[/dim]")
            console.print("[dim]  • Use --filter all to show all DAGs[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

# Create server subcommand group
server = typer.Typer(help="Server management commands")
app.add_typer(server, name="server")

@server.command("start")
def start_server(
    host: str = typer.Option("0.0.0.0", "--host", help="Server host"),
    port: int = typer.Option(8000, "--port", help="Server port"),
    log_level: str = typer.Option("info", "--log-level", help="Log level"),
    daemon: bool = typer.Option(False, "--daemon", help="Run as daemon")
):
    """Start the Maestro API server"""
    if daemon:
        # Run server as daemon
        cmd = [
            sys.executable, "-m", "maestro.server.app",
            "--host", host,
            "--port", str(port),
            "--log-level", log_level
        ]

        process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        pid_file = os.path.expanduser("~/.maestro/server.pid")
        os.makedirs(os.path.dirname(pid_file), exist_ok=True)
        with open(pid_file, "w") as f:
            f.write(str(process.pid))

        status_data = {
            "pid": process.pid,
            "host": host,
            "port": port
        }
        with open(pid_file, "w") as f:
            json.dump(status_data, f)

        # Wait for server to start
        api_client.base_url = f"http://{host}:{port}"
        if api_client.wait_for_server(30):
            console.print(f"[green]Maestro server started on {host}:{port}[/green]")

            try:
                with open(pid_file, "r") as f:
                    data = json.load(f)
                pid = data["pid"]
                host = data.get("host", "unknown")
                port = data.get("port", "unknown")

                console.print(f"[blue]PID file created at {pid_file} with number: {pid}[/blue]")

            except Exception as e:
                console.print(f"[red]Error reading PID file: {e}[/red]")
                raise typer.Exit(1)

        else:
            console.print("[red]Failed to start server[/red]")
            raise typer.Exit(1)

    else:
        # Run server in foreground
        from maestro.server.app import start_server
        console.print(f"[green]Starting Maestro server on {host}:{port}[/green]")
        start_server(host, port, log_level)


@server.command("stop")
def stop_server():
    """Stop the Maestro API server"""
    pid_file = os.path.expanduser("~/.maestro/server.pid")

    if not os.path.exists(pid_file):
        console.print("[red]No PID file found. Is the server running as a daemon?[/red]")
        raise typer.Exit(1)

    try:
        with open(pid_file, "r") as f:
            data = json.load(f)
        pid = data["pid"]
        host = data.get("host", "unknown")
        port = data.get("port", "unknown")
    except Exception as e:
        console.print(f"[red]Error reading PID file: {e}[/red]")
        raise typer.Exit(1)

    try:
        os.kill(pid, signal.SIGTERM)
        console.print(f"[green]Stopping Maestro server (PID {pid}), currently running on {host}:{port}...[/green]")
    except ProcessLookupError:
        console.print("[yellow]Process not found. Removing stale PID file.[/yellow]")
    except PermissionError:
        console.print("[red]Permission denied to stop server process.[/red]")
        raise typer.Exit(1)
    finally:
        if os.path.exists(pid_file):
            os.remove(pid_file)

    console.print("[green]Server stopped successfully.[/green]")


@server.command("status")
def server_status(
    server_url: str = typer.Option("http://localhost:8000",
                                   "--server",
                                   help="Maestro server URL")
):
    """Check server status"""
    api_client.base_url = server_url
    
    try:
        response = api_client.health_check()
        console.print(f"[green]Server is running at {server_url}[/green]")
        console.print(f"[dim]Status: {response['status']}[/dim]")
        console.print(f"[dim]Timestamp: {response['timestamp']}[/dim]")
    except (ConnectionError, TimeoutError):
        console.print(f"[red]Server is not running at {server_url}[/red]")
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
