import networkx as nx
import time
from rich.console import Console, Group
from rich import get_console
from rich.text import Text
from rich.panel import Panel
from rich.live import Live
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.layout import Layout


class StatusManager:
    def __init__(self, dag: nx.DiGraph):
        self.dag = dag
        self.task_statuses = {node: "pending" for node in dag.nodes()}
        self.task_start_times = {}
        self.task_end_times = {}

    def set_task_status(self, task_name: str, status: str):
        if task_name in self.task_statuses:
            self.task_statuses[task_name] = status
            if status == "running":
                self.task_start_times[task_name] = time.time()
            elif status in ["completed", "failed", "skipped"]:
                self.task_end_times[task_name] = time.time()

    def get_task_status(self, task_name: str):
        return self.task_statuses.get(task_name, "unknown")

    def get_task_duration(self, task_name: str):
        start = self.task_start_times.get(task_name)
        end = self.task_end_times.get(task_name)
        if start and end:
            return end - start
        elif start and self.get_task_status(task_name) == "running":
            return time.time() - start
        return None


class ProgressTracker:
    def __init__(self, total_tasks: int):
        self.total_tasks = total_tasks
        self.completed_tasks = 0
        self.start_time = time.time()

    def increment_completed(self):
        self.completed_tasks += 1

    def get_progress_percentage(self):
        if self.total_tasks == 0:
            return 0
        return (self.completed_tasks / self.total_tasks) * 100

    def get_elapsed_time(self):
        return time.time() - self.start_time

    def get_estimated_time_remaining(self):
        elapsed = self.get_elapsed_time()
        if self.completed_tasks == 0:
            return None
        tasks_per_second = self.completed_tasks / elapsed
        if tasks_per_second == 0:
            return None
        remaining_tasks = self.total_tasks - self.completed_tasks
        return remaining_tasks / tasks_per_second

    def get_execution_rate(self):
        elapsed = self.get_elapsed_time()
        if elapsed == 0:
            return 0
        return self.completed_tasks / (elapsed / 60)  # tasks per minute


class DisplayManager:
    def __init__(self, dag: nx.DiGraph, status_manager: StatusManager, progress_tracker: ProgressTracker):
        self.dag = dag
        self.status_manager = status_manager
        self.progress_tracker = progress_tracker
        self.console = get_console()
        self.progress = Progress(
            BarColumn(bar_width=None),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "(", TextColumn("{task.completed}/{task.total}"), ")",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
        self.progress_task_id = self.progress.add_task("progress", total=self.progress_tracker.total_tasks,
                                                       completed=self.progress_tracker.completed_tasks)

    def _calculate_level_groups(self):
        level_groups = {}
        all_nodes = set(self.dag.nodes())
        processed_nodes = set()
        current_level = 0

        while processed_nodes != all_nodes:
            current_level_nodes = []
            for node in all_nodes - processed_nodes:
                predecessors = set(self.dag.predecessors(node))
                if predecessors.issubset(processed_nodes):
                    current_level_nodes.append(node)

            if not current_level_nodes:
                # Handle cycles or other issues
                remaining_nodes = list(all_nodes - processed_nodes)
                if remaining_nodes:
                    level_groups[current_level] = remaining_nodes
                break  # Exit loop

            level_groups[current_level] = current_level_nodes
            processed_nodes.update(current_level_nodes)
            current_level += 1
        return level_groups

    def _get_status_symbol(self, status: str):
        symbols = {
            "pending": "⏳",
            "running": "⚡",
            "completed": "✅",
            "failed": "❌",
            "skipped": "⏭️",
            "waiting": "○",
        }
        return symbols.get(status, "?")

    def _calculate_task_display_width(self, node):
        """Calculate the width needed for a task box"""
        status = self.status_manager.get_task_status(node)
        symbol = self._get_status_symbol(status)
        duration = self.status_manager.get_task_duration(node)
        duration_str = f" ({duration:.1f}s)" if duration else ""
        # Use rich.text.Text to correctly measure width of all characters, including symbols
        task_text = Text(f" {symbol} {node}{duration_str} ")
        return task_text.cell_len + 2  # Add 2 for borders

    def _render_dag_visualization(self):
        """Render an elegant horizontal DAG with rich styling"""

        if not self.dag.nodes():
            return Panel(
                Text("No tasks in DAG.", style="dim"),
                title=Text("DAG Visualization", style="bold magenta"),
                border_style="blue",
                expand=True,
            )

        # Calculate levels using the new method
        level_groups = self._calculate_level_groups()

        # Create rich content
        content_parts = []

        # Title
        content_parts.append(Text("🔄 Execution Flow", style="bold cyan"))
        content_parts.append(Text(""))

        # Build horizontal flow - ensure we show ALL levels
        max_level = max(level_groups.keys()) if level_groups else -1

        for level in range(max_level + 1):
            if level not in level_groups or not level_groups[level]:
                continue

            level_nodes = level_groups[level]

            # Level header
            level_text = Text(f"Level {level + 1}:", style="bold white")
            content_parts.append(level_text)

            # Create a table for the level
            grid = Table.grid(expand=True)
            for _ in level_nodes:
                grid.add_column()

            # Prepare row content
            top_row = []
            middle_row = []
            bottom_row = []

            for node in level_nodes:
                status = self.status_manager.get_task_status(node)
                box_style = self._get_box_style(status)
                width = self._calculate_task_display_width(node)
                symbol = self._get_status_symbol(status)
                duration = self.status_manager.get_task_duration(node)
                duration_str = f" ({duration:.1f}s)" if duration else ""
                task_content = f" {symbol} {node}{duration_str} "

                top_row.append(f"┌{'─' * (width - 2)}┐")
                middle_row.append(f"│{task_content.center(width - 2)}│")
                bottom_row.append(f"└{'─' * (width - 2)}┘")

            grid.add_row(*[Text(item, style=self._get_box_style(self.status_manager.get_task_status(node))) for item, node in zip(top_row, level_nodes)])
            grid.add_row(*[Text(item, style=self._get_box_style(self.status_manager.get_task_status(node))) for item, node in zip(middle_row, level_nodes)])
            grid.add_row(*[Text(item, style=self._get_box_style(self.status_manager.get_task_status(node))) for item, node in zip(bottom_row, level_nodes)])

            content_parts.append(grid)

            # Add arrows to next level
            if level < max_level:
                arrow_grid = Table.grid(expand=True)
                for _ in level_nodes:
                    arrow_grid.add_column()

                arrow_row1 = []
                arrow_row2 = []
                for node in level_nodes:
                    width = self._calculate_task_display_width(node)
                    arrow_row1.append(" " * (width // 2) + "│" + " " * (width - width // 2 - 1))
                    arrow_row2.append(" " * (width // 2) + "▼" + " " * (width - width // 2 - 1))

                arrow_grid.add_row(*[Text(item, style="cyan") for item in arrow_row1])
                arrow_grid.add_row(*[Text(item, style="cyan") for item in arrow_row2])
                content_parts.append(arrow_grid)

            content_parts.append(Text(""))  # Empty line

        # Add legend
        content_parts.append(Text(""))
        content_parts.append(Text("Legend:", style="bold white"))
        legend = Text()
        legend.append("⏳ Pending  ", style="dim")
        legend.append("⚡ Running  ", style="yellow")
        legend.append("✅ Completed  ", style="green")
        legend.append("❌ Failed  ", style="red")
        legend.append("⏭️ Skipped", style="blue")
        content_parts.append(legend)

        # Combine all content
        content = Group(*content_parts)

        return Panel(
            content,
            title=Text("DAG Visualization", style="bold magenta"),
            border_style="blue",
            expand=True,
            padding=(1, 2),
        )

    def _get_box_style(self, status):
        """Get the appropriate box style for a task status"""
        if status == "completed":
            return "green"
        elif status == "running":
            return "yellow"
        elif status == "failed":
            return "red"
        else:
            return "blue"

    def _render_execution_status(self):
        status_renderables = []
        status_renderables.append(Text("Execution Status:", style="bold magenta"))
        for task_name in self.dag.nodes():
            status = self.status_manager.get_task_status(task_name)
            symbol = self._get_status_symbol(status)
            duration = self.status_manager.get_task_duration(task_name)
            duration_str = f" ({duration:.1f}s)" if duration is not None else ""
            status_renderables.append(Text(f"{symbol} {task_name} ({status}{duration_str})"))
        return Group(*status_renderables)

    def _render_overall_progress(self):
        progress_renderables = []
        progress_renderables.append(Text("Overall Progress:", style="bold magenta"))

        self.progress.update(self.progress_task_id, completed=self.progress_tracker.completed_tasks)

        progress_renderables.append(self.progress)

        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(self.progress_tracker.get_elapsed_time()))
        eta_str = time.strftime("%H:%M:%S", time.gmtime(
            self.progress_tracker.get_estimated_time_remaining())) if self.progress_tracker.get_estimated_time_remaining() is not None else "N/A"
        rate_str = f"{self.progress_tracker.get_execution_rate():.1f}/min"

        progress_renderables.append(Text(f"⏱️  Elapsed: {elapsed_str} | ETA: {eta_str} | Rate: {rate_str}"))
        return Group(*progress_renderables)

    def display(self):
        layout = Layout(name="root")

        layout.split(
            Layout(name="header", size=3),
            Layout(name="body"),
        )

        layout["header"].update(Panel(
            Text("Maestro Workflow Manager", justify="center", style="bold green"),
            border_style="green",
            expand=True
        ))

        # The body is now split vertically. The DAG panel will be scrollable.
        layout["body"].split_row(
            Layout(name="dag_viz"),
            Layout(name="side_panel", size=40),  # Give side panel a fixed size
        )

        # The side panel contains status and progress
        layout["side_panel"].split(
            Layout(name="execution_status"),
            Layout(name="overall_progress"),
        )

        # The DAG visualization is now the main, scrollable panel
        dag_renderable = self._render_dag_visualization()
        layout["dag_viz"].update(dag_renderable)

        layout["execution_status"].update(self._render_execution_status())
        layout["overall_progress"].update(self._render_overall_progress())

        return layout