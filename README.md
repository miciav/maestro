
# Maestro - Python Task Orchestrator

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.13](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/downloads/release/python-3130/)


Maestro is a simple Python task orchestrator that runs a series of tasks defined in a DAG (Directed Acyclic Graph).

## Features

- **DAG-based task execution:** Define complex workflows with dependencies.
- **YAML configuration:** Easily define your DAGs in a human-readable format.
- **Extensible:** Create your own custom task types.
- **CLI interface:** Run, validate, and visualize your DAGs from the command line.
- **Real-time updates:** (Coming soon) Get real-time updates on task status.

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/maestro.git
cd maestro

# Install dependencies (including test dependencies) using uv
uv sync --extra test

# Install the project in editable mode for local development
uv pip install -e .
```

## Usage

### Running a DAG

```bash
uv run -m maestro.cli run examples/full_sample_dag.yaml
```

### Validating a DAG

```bash
uv run -m maestro.cli validate examples/sample_dag.yaml
```

### Visualizing a DAG

```bash
uv run -m maestro.cli visualize examples/sample_dag.yaml
```

### Getting DAG Status

```bash
uv run -m maestro.cli status examples/sample_dag.yaml
```

## Configuration

DAGs are defined in YAML files. Here's an example:

```yaml
dag:
  name: "esempio_dag"
  tasks:
    - task_id: "task_1"
      type: "PrintTask"
      params:
        message: "Inizio pipeline"
        delay: 2
      dependencies: []
    
    - task_id: "task_2"
      type: "FileWriterTask"
      params:
        filepath: "output.txt"
        content: "Risultato elaborazione"
      dependencies: ["task_1"]
    
    - task_id: "task_3"
      type: "PrintTask"
      params:
        message: "Fine pipeline"
      dependencies: ["task_2"]
```

## Creating Custom Tasks

To create a custom task, you need to:

1.  Create a new class that inherits from `maestro.tasks.base.BaseTask`.
2.  Define the parameters for your task as Pydantic fields.
3.  Implement the `execute` method.
4.  Register your new task in the `Orchestrator`'s `task_types` dictionary.

## Class Diagram

```mermaid
classDiagram
    direction LR
    class Task {
        <<abstract>>
        +str task_id
        +List~str~ dependencies
        +TaskStatus status
        +Callable on_success
        +Callable on_failure
        +execute()
    }

    class BaseTask {
        +execute()
    }

    class PrintTask {
        +str message
        +Optional~int~ delay
        +execute()
    }

    class FileWriterTask {
        +str filepath
        +str content
        +Literal~"append", "overwrite"~ mode
        +execute()
    }

    class WaitTask {
        +int delay
        +execute()
    }

    class DAG {
        +Dict~str, Task~ tasks
        +add_task(task: Task)
        +validate()
        +get_execution_order() List~str~
        +execute()
    }

    class Orchestrator {
        +Dict~str, type~ task_types
        +load_dag_from_file(filepath: str) DAG
        +run_dag(dag: DAG)
        +visualize_dag(dag: DAG)
        +get_dag_status(dag: DAG) Dict~str, Any~
    }

    Task <|-- BaseTask
    BaseTask <|-- PrintTask
    BaseTask <|-- FileWriterTask
    BaseTask <|-- WaitTask
    DAG "1" -- "*" Task : contains
    Orchestrator ..> DAGLoader : uses
    DAGLoader ..> DAG : creates
    Orchestrator ..> Task : manages
    Orchestrator ..> BaseTask : uses
    Orchestrator ..> PrintTask : uses
    Orchestrator ..> FileWriterTask : uses
    Orchestrator ..> WaitTask : uses
```

## Running Tests

To run all tests, use the provided script:

```bash
./run_tests.sh
```
