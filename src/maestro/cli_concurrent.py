#!/usr/bin/env python3

import typer
from typing import Optional, List
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.text import Text
import time
import threading
import signal
import sys
from datetime import datetime, timedelta

from maestro.core.orchestrator import Orchestrator
from maestro.core.status_manager import StatusManager

app = typer.Typer(help="Maestro - Multi-threaded DAG execution orchestrator")
console = Console()

@app.command()
def attach(
    dag_id: str = typer.Argument(..., help="DAG ID to attach logs for"),
    execution_id: Optional[str] = typer.Option(None, "--execution-id", help="Specific execution ID to attach"),
    task_filter: Optional[str] = typer.Option(None, "--task", help="Filter logs by task ID"),
    level_filter: Optional[str] = typer.Option(None, "--level", help="Filter logs by level (DEBUG, INFO, WARNING, ERROR)"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """Attach to execution logs for a running DAG."""
    orchestrator = Orchestrator(db_path=db_path)

    def handle_exit(signum, frame):
        console.print("\n[yellow]Detachment successful. Returning to CLI.[/yellow]")
        sys.exit(0)

    # Bind the signals for detachment
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    console.print(f"[bold cyan]Attaching to live logs for DAG: {dag_id}[/bold cyan]")
    if execution_id:
        console.print(f"[dim]Execution ID: {execution_id}[/dim]")
    console.print(f"[dim]Press Ctrl+C to detach and return to CLI[/dim]\n")

    last_timestamp = None
    displayed_logs = set()  # Track displayed log entries to avoid duplicates

    try:
        while True:
            with orchestrator.status_manager as sm:
                logs = sm.get_execution_logs(dag_id, execution_id, limit=100)

                # Apply filters
                if task_filter:
                    logs = [log for log in logs if log["task_id"] == task_filter]
                if level_filter:
                    logs = [log for log in logs if log["level"].upper() == level_filter.upper()]

                # Filter for new logs only
                new_logs = []
                for log in logs:
                    # Create a unique identifier for each log entry
                    log_id = f"{log['timestamp']}_{log['task_id']}_{log['level']}_{log['message'][:50]}"
                    
                    if log_id not in displayed_logs:
                        if last_timestamp is None or log["timestamp"] > last_timestamp:
                            new_logs.append(log)
                            displayed_logs.add(log_id)
                            last_timestamp = log["timestamp"]

                # Display new logs in chronological order (oldest first)
                if new_logs:
                    for log in reversed(new_logs):  # Reverse because logs come in DESC order
                        level_style = {
                            "ERROR": "red",
                            "WARNING": "yellow",
                            "INFO": "green",
                            "DEBUG": "blue"
                        }.get(log["level"], "white")

                        timestamp = log["timestamp"].split("T")[1].split(".")[0] if "T" in log["timestamp"] else log["timestamp"]
                        task_id = log["task_id"] or "system"

                        console.print(f"[dim]{timestamp}[/dim] [{level_style}]{log['level']}[/{level_style}] [magenta]{task_id}[/magenta]: {log['message']}")

                # Clean up displayed_logs set to prevent memory issues
                if len(displayed_logs) > 1000:
                    displayed_logs.clear()
                    last_timestamp = None

                time.sleep(1)  # Check for new logs every second

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")

@app.command()
def run_async(
    dag_file: str = typer.Argument(..., help="Path to the DAG YAML file"),
    resume: bool = typer.Option(False, "--resume", help="Resume from last checkpoint"),
    fail_fast: bool = typer.Option(True, "--fail-fast/--no-fail-fast", help="Stop on first failure"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path"),
    log_level: str = typer.Option("INFO", "--log-level", help="Log level")
):
    """Run a DAG asynchronously in a separate thread."""
    # Set log level to INFO for async operations but disable rich handler
    orchestrator = Orchestrator(log_level=log_level, db_path=db_path)
    
    try:
        dag = orchestrator.load_dag_from_file(dag_file)
        console.print(f"[bold green]Loading DAG:[/bold green] {dag.dag_id}")
        
        # Disable rich logging for async execution
        orchestrator.disable_rich_logging()
        
        # Start DAG execution in background thread
        execution_id = orchestrator.run_dag_in_thread(
            dag=dag,
            resume=resume,
            fail_fast=fail_fast
        )
        
        # Enhanced confirmation message with essential information
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        console.print()
        console.print("[bold green]✓ DAG submitted successfully![/bold green]")
        console.print(f"[cyan]DAG ID:[/cyan] {dag.dag_id}")
        console.print(f"[cyan]Execution ID:[/cyan] {execution_id}")
        console.print(f"[cyan]Status:[/cyan] [yellow]running[/yellow]")
        console.print(f"[cyan]Submitted at:[/cyan] {current_time}")
        console.print()
        console.print("[bold blue]Available commands:[/bold blue]")
        console.print(f"  • [bold]maestro status {dag.dag_id}[/bold] - Check DAG status")
        console.print(f"  • [bold]maestro logs {dag.dag_id}[/bold] - View historical logs")
        console.print(f"  • [bold]maestro attach {dag.dag_id}[/bold] - Attach to live log stream")
        console.print(f"  • [bold]maestro monitor {dag.dag_id}[/bold] - Monitor execution progress")
        console.print()

        return  # Return immediately to prevent waiting on the thread execution

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)

@app.command()
def run(
    dag_file: str = typer.Argument(..., help="Path to the DAG YAML file"),
    resume: bool = typer.Option(False, "--resume", help="Resume from last checkpoint"),
    fail_fast: bool = typer.Option(True, "--fail-fast/--no-fail-fast", help="Stop on first failure"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path"),
    log_level: str = typer.Option("INFO", "--log-level", help="Log level"),
    modern_ui: bool = typer.Option(False, "--modern-ui", help="Enable modern UI (deprecated, runs async by default)")
):
    """Run a DAG (runs asynchronously by default)."""
    if modern_ui:
        console.print("[yellow]Note: --modern-ui is deprecated. All DAGs now run asynchronously with enhanced UI.[/yellow]")
    
    # Call the run_async function
    run_async(dag_file, resume, fail_fast, db_path, log_level)

@app.command()
def monitor(
    dag_id: str = typer.Argument(..., help="DAG ID to monitor"),
    execution_id: Optional[str] = typer.Option(None, "--execution-id", help="Specific execution ID"),
    refresh_interval: int = typer.Option(1, "--refresh", help="Refresh interval in seconds"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """Monitor DAG execution in real-time."""
    orchestrator = Orchestrator(db_path=db_path)
    
    def create_status_display():
        with orchestrator.status_manager as sm:
            exec_details = sm.get_dag_execution_details(dag_id, execution_id)
            
            if not exec_details:
                return Panel("[red]No execution found[/red]", title="DAG Status")
            
            # Create main status table
            table = Table(title=f"DAG Status: {dag_id}")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="magenta")
            
            table.add_row("Execution ID", exec_details.get("execution_id", "N/A"))
            table.add_row("Status", exec_details.get("status", "N/A"))
            table.add_row("Started", exec_details.get("started_at", "N/A"))
            table.add_row("Completed", exec_details.get("completed_at", "N/A") or "Running...")
            table.add_row("Thread ID", str(exec_details.get("thread_id", "N/A")))
            
            # Create tasks table
            tasks_table = Table(title="Tasks Status")
            tasks_table.add_column("Task ID", style="cyan")
            tasks_table.add_column("Status", style="magenta")
            tasks_table.add_column("Started", style="green")
            tasks_table.add_column("Completed", style="yellow")
            
            for task in exec_details.get("tasks", []):
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
            
            return Panel.fit(
                f"{table}\n\n{tasks_table}",
                title=f"DAG Monitor - {dag_id}",
                border_style="bright_blue"
            )
    
    try:
        with Live(create_status_display(), refresh_per_second=1/refresh_interval, console=console) as live:
            while True:
                time.sleep(refresh_interval)
                live.update(create_status_display())
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def status(
    dag_id: Optional[str] = typer.Argument(None, help="DAG ID to check status"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """Show status of DAG executions."""
    orchestrator = Orchestrator(db_path=db_path)
    
    try:
        with orchestrator.status_manager as sm:
            if dag_id:
                # Show specific DAG status
                exec_details = sm.get_dag_execution_details(dag_id)
                if not exec_details:
                    console.print(f"[red]No execution found for DAG: {dag_id}[/red]")
                    return
                
                table = Table(title=f"DAG Status: {dag_id}")
                table.add_column("Property", style="cyan")
                table.add_column("Value", style="magenta")
                
                table.add_row("Execution ID", exec_details.get("execution_id", "N/A"))
                table.add_row("Status", exec_details.get("status", "N/A"))
                table.add_row("Started", exec_details.get("started_at", "N/A"))
                table.add_row("Completed", exec_details.get("completed_at", "N/A") or "Running...")
                
                console.print(table)
                
                # Show tasks status
                tasks_table = Table(title="Tasks")
                tasks_table.add_column("Task ID", style="cyan")
                tasks_table.add_column("Status", style="magenta")
                tasks_table.add_column("Started", style="green")
                tasks_table.add_column("Completed", style="yellow")
                
                for task in exec_details.get("tasks", []):
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
                
            else:
                # Show all running DAGs
                running_dags = sm.get_running_dags()
                if not running_dags:
                    console.print("[yellow]No running DAGs found[/yellow]")
                    return
                
                table = Table(title="Running DAGs")
                table.add_column("DAG ID", style="cyan")
                table.add_column("Execution ID", style="magenta")
                table.add_column("Started", style="green")
                table.add_column("Thread ID", style="yellow")
                
                for dag in running_dags:
                    table.add_row(
                        dag["dag_id"],
                        dag["execution_id"],
                        dag["started_at"],
                        str(dag["thread_id"])
                    )
                
                console.print(table)
                
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def logs(
    dag_id: str = typer.Argument(..., help="DAG ID to get logs for"),
    execution_id: Optional[str] = typer.Option(None, "--execution-id", help="Specific execution ID"),
    limit: int = typer.Option(100, "--limit", help="Number of log entries to show"),
    follow: bool = typer.Option(False, "--follow", "-f", help="Follow log output in real-time"),
    task_filter: Optional[str] = typer.Option(None, "--task", help="Filter logs by task ID"),
    level_filter: Optional[str] = typer.Option(None, "--level", help="Filter logs by level (DEBUG, INFO, WARNING, ERROR)"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """Show execution logs for a DAG with optional real-time following."""
    orchestrator = Orchestrator(db_path=db_path)
    
    if follow:
        # Use the new attach command for live streaming
        console.print(f"[yellow]Note: Using --follow is equivalent to 'maestro attach {dag_id}'[/yellow]")
        return attach(dag_id, execution_id, task_filter, level_filter, db_path)
    
    try:
        with orchestrator.status_manager as sm:
            logs = sm.get_execution_logs(dag_id, execution_id, limit)
            
            if not logs:
                console.print(f"[yellow]No logs found for DAG: {dag_id}[/yellow]")
                return
            
            # Apply filters
            if task_filter:
                logs = [log for log in logs if log["task_id"] == task_filter]
            if level_filter:
                logs = [log for log in logs if log["level"].upper() == level_filter.upper()]
            
            if not logs:
                console.print(f"[yellow]No logs found matching the specified filters[/yellow]")
                return
            
            # Enhanced display with better formatting
            console.print(f"[bold cyan]Historical Logs: {dag_id}[/bold cyan]")
            if execution_id:
                console.print(f"[dim]Execution ID: {execution_id}[/dim]")
            console.print(f"[dim]Showing {len(logs)} log entries[/dim]")
            console.print()
            
            for log in reversed(logs):  # Show newest first
                level_style = {
                    "ERROR": "red",
                    "WARNING": "yellow",
                    "INFO": "green",
                    "DEBUG": "blue"
                }.get(log["level"], "white")
                
                timestamp = log["timestamp"].split("T")[1].split(".")[0] if "T" in log["timestamp"] else log["timestamp"]
                task_id = log["task_id"] or "system"
                
                console.print(f"[dim]{timestamp}[/dim] [{level_style}]{log['level']}[/{level_style}] [magenta]{task_id}[/magenta]: {log['message']}")
            
            console.print()
            console.print(f"[dim]💡 Tip: Use 'maestro attach {dag_id}' for live log streaming[/dim]")
            
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def validate(
    dag_file: str = typer.Argument(..., help="Path to the DAG YAML file"),
    log_level: str = typer.Option("CRITICAL", "--log-level", help="Log level")
):
    """Validate a DAG file."""
    orchestrator = Orchestrator(log_level=log_level)
    
    try:
        dag = orchestrator.load_dag_from_file(dag_file)
        console.print(f"[bold green]✓ DAG is valid:[/bold green] {dag.dag_id}")
        
        # Show DAG structure
        table = Table(title="DAG Structure")
        table.add_column("Task ID", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Dependencies", style="green")
        
        execution_order = dag.get_execution_order()
        for task_id in execution_order:
            task = dag.tasks[task_id]
            deps = ", ".join(task.dependencies) if task.dependencies else "None"
            table.add_row(task_id, task.__class__.__name__, deps)
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[bold red]✗ DAG validation failed:[/bold red] {e}")
        raise typer.Exit(1)

@app.command()
def visualize(
    dag_file: str = typer.Argument(..., help="Path to the DAG YAML file"),
    log_level: str = typer.Option("CRITICAL", "--log-level", help="Log level")
):
    """Visualize a DAG structure (alias for validate)."""
    # Call the validate function
    validate(dag_file, log_level)

@app.command()
def list_dags(
    status_filter: Optional[str] = typer.Option(None, "--status", help="Filter by status (running, completed, failed, cancelled)"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """List all DAGs that have been executed."""
    orchestrator = Orchestrator(db_path=db_path)
    
    try:
        with orchestrator.status_manager as sm:
            if status_filter:
                # Get DAGs with specific status
                dags = sm.get_dags_by_status(status_filter)
                title = f"DAGs with status: {status_filter}"
            else:
                # Get all DAGs
                dags = sm.get_all_dags()
                title = "All DAGs in Database"
            
            if not dags:
                console.print(f"[yellow]No DAGs found{' with status ' + status_filter if status_filter else ''}[/yellow]")
                return
                
            table = Table(title=title)
            table.add_column("DAG ID", style="cyan")
            table.add_column("Execution ID", style="blue")
            table.add_column("Status", style="green")
            table.add_column("Started", style="magenta")
            table.add_column("Completed", style="yellow")
            table.add_column("Thread ID", style="dim")
            
            for dag in dags:
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
                    dag.get("completed_at", "N/A") or "Running...",
                    str(dag.get("thread_id", "N/A"))
                )
            
            console.print(table)
            
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def summary(
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """Show summary statistics of all DAGs."""
    orchestrator = Orchestrator(db_path=db_path)
    
    try:
        with orchestrator.status_manager as sm:
            summary_data = sm.get_dag_summary()
            
            # Create summary table
            table = Table(title="DAG Execution Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="magenta")
            
            table.add_row("Total Executions", str(summary_data["total_executions"]))
            table.add_row("Unique DAGs", str(summary_data["unique_dags"]))
            
            console.print(table)
            
            # Create status breakdown table
            status_table = Table(title="Status Breakdown")
            status_table.add_column("Status", style="cyan")
            status_table.add_column("Count", style="magenta")
            status_table.add_column("Percentage", style="green")
            
            total = summary_data["total_executions"]
            for status, count in summary_data["status_counts"].items():
                percentage = (count / total * 100) if total > 0 else 0
                status_style = {
                    "completed": "green",
                    "running": "yellow",
                    "failed": "red",
                    "cancelled": "orange1"
                }.get(status, "white")
                
                status_table.add_row(
                    f"[{status_style}]{status}[/{status_style}]",
                    str(count),
                    f"{percentage:.1f}%"
                )
            
            console.print(status_table)
            
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def history(
    dag_id: str = typer.Argument(..., help="DAG ID to get history for"),
    limit: int = typer.Option(10, "--limit", help="Number of executions to show"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """Show execution history for a specific DAG."""
    orchestrator = Orchestrator(db_path=db_path)
    
    try:
        with orchestrator.status_manager as sm:
            history_data = sm.get_dag_history(dag_id)
            
            if not history_data:
                console.print(f"[yellow]No execution history found for DAG: {dag_id}[/yellow]")
                return
            
            # Limit results
            history_data = history_data[:limit]
            
            table = Table(title=f"Execution History: {dag_id}")
            table.add_column("Execution ID", style="cyan")
            table.add_column("Status", style="green")
            table.add_column("Started", style="magenta")
            table.add_column("Completed", style="yellow")
            table.add_column("Duration", style="blue")
            table.add_column("Thread ID", style="dim")
            
            for execution in history_data:
                status_style = {
                    "completed": "green",
                    "running": "yellow",
                    "failed": "red",
                    "cancelled": "orange1"
                }.get(execution.get("status"), "white")
                
                # Calculate duration
                duration = "N/A"
                if execution.get("started_at") and execution.get("completed_at"):
                    try:
                        start = datetime.fromisoformat(execution["started_at"])
                        end = datetime.fromisoformat(execution["completed_at"])
                        duration = str(end - start).split('.')[0]  # Remove microseconds
                    except:
                        duration = "N/A"
                
                table.add_row(
                    execution["execution_id"][:8] + "...",
                    f"[{status_style}]{execution.get('status', 'N/A')}[/{status_style}]",
                    execution.get("started_at", "N/A"),
                    execution.get("completed_at", "N/A") or "Running...",
                    duration,
                    str(execution.get("thread_id", "N/A"))
                )
            
            console.print(table)
            
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def cleanup(
    days: int = typer.Option(30, "--days", help="Days to keep (older executions will be deleted)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without actually deleting"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """Clean up old execution records."""
    orchestrator = Orchestrator(db_path=db_path)
    
    try:
        with orchestrator.status_manager as sm:
            if dry_run:
                # Calculate what would be deleted
                cutoff_date = datetime.now() - timedelta(days=days)
                cutoff_iso = cutoff_date.isoformat()
                
                cursor = sm._conn.execute(
                    "SELECT COUNT(*) FROM dag_executions WHERE started_at < ?", 
                    (cutoff_iso,)
                )
                count_to_delete = cursor.fetchone()[0]
                
                console.print(f"[yellow]Dry run: Would delete {count_to_delete} execution records older than {days} days[/yellow]")
                console.print(f"[yellow]Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}[/yellow]")
            else:
                deleted_count = sm.cleanup_old_executions(days)
                console.print(f"[green]Deleted {deleted_count} execution records older than {days} days[/green]")
                
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def cancel(
    dag_id: str = typer.Argument(..., help="DAG ID to cancel"),
    execution_id: Optional[str] = typer.Option(None, "--execution-id", help="Specific execution ID to cancel"),
    db_path: str = typer.Option("maestro.db", "--db", help="Database path")
):
    """Cancel a running DAG execution."""
    orchestrator = Orchestrator(db_path=db_path)
    
    try:
        with orchestrator.status_manager as sm:
            success = sm.cancel_dag_execution(dag_id, execution_id)
            
            if success:
                if execution_id:
                    console.print(f"[green]Successfully cancelled execution {execution_id} of DAG {dag_id}[/green]")
                else:
                    console.print(f"[green]Successfully cancelled all running executions of DAG {dag_id}[/green]")
            else:
                console.print(f"[yellow]No running executions found for DAG {dag_id}[/yellow]")
                
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
