#!/usr/bin/env python3

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uvicorn
import asyncio
import json
import time
from datetime import datetime
import logging
import threading
from contextlib import asynccontextmanager

from maestro.core.orchestrator import Orchestrator
from maestro.core.status_manager import StatusManager
import random
import string
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global orchestrator instance
orchestrator = None

# Docker-like name generator - lists of adjectives and nouns
DOCKER_ADJECTIVES = [
    "amazing", "awesome", "blissful", "bold", "brave", "charming", "clever", "cool", "dazzling", "determined",
    "eager", "ecstatic", "elegant", "epic", "exciting", "fantastic", "friendly", "gallant", "gentle", "gracious",
    "happy", "hardcore", "inspiring", "jolly", "keen", "kind", "laughing", "loving", "lucid", "magical",
    "modest", "naughty", "nervous", "nice", "objective", "optimistic", "peaceful", "pedantic", "pensive", "practical",
    "quirky", "relaxed", "romantic", "serene", "sharp", "stoic", "sweet", "tender", "thirsty", "trusting",
    "unruffled", "upbeat", "vibrant", "vigilant", "wonderful", "xenial", "youthful", "zealous", "zen"
]

DOCKER_NOUNS = [
    "albattani", "allen", "almeida", "antonelli", "archimedes", "ardinghelli", "aryabhata", "austin", "babbage", "banach",
    "banzai", "bardeen", "bartik", "bassi", "beaver", "bell", "benz", "bhabha", "bhaskara", "black",
    "blackburn", "blackwell", "bohr", "booth", "borg", "bose", "bouman", "boyd", "brahmagupta", "brattain",
    "brown", "buck", "burnell", "cannon", "carson", "cartwright", "cerf", "chandrasekhar", "chaplygin", "chatelet",
    "chatterjee", "chebyshev", "cohen", "chaum", "clarke", "colden", "cori", "cray", "curran", "curie",
    "darwin", "davinci", "dewdney", "dhawan", "diffie", "dijkstra", "dirac", "driscoll", "dubinsky", "easley",
    "edison", "einstein", "elbakyan", "elgamal", "elion", "ellis", "engelbart", "euclid", "euler", "faraday",
    "feistel", "fermat", "fermi", "feynman", "franklin", "gagarin", "galileo", "galois", "ganguly", "gates",
    "gauss", "germain", "goldberg", "goldstine", "goldwasser", "golick", "goodall", "gould", "greider", "grothendieck",
    "haibt", "hamilton", "haslett", "hawking", "heisenberg", "hermann", "herschel", "hertz", "heyrovsky", "hodgkin",
    "hofstadter", "hoover", "hopper", "hugle", "hypatia", "ishizaka", "jackson", "jang", "jemison", "jennings",
    "jepsen", "johnson", "joliot", "jones", "kalam", "kapitsa", "kare", "keldysh", "keller", "kepler",
    "khorana", "kilby", "kirch", "knuth", "kowalevski", "lalande", "lamarr", "lamport", "leakey", "leavitt",
    "lederberg", "lehmann", "lewin", "lichterman", "liskov", "lovelace", "lumiere", "mahavira", "margulis", "matsumoto",
    "maxwell", "mayer", "mccarthy", "mcclintock", "mclaren", "mclean", "mcnulty", "mendel", "mendeleev", "menshov",
    "merkle", "mestorf", "mirzakhani", "moore", "morse", "murdoch", "moser", "napier", "nash", "neumann",
    "newton", "nightingale", "nobel", "noether", "northcutt", "noyce", "panini", "pare", "pascal", "pasteur",
    "payne", "perlman", "pike", "poincare", "poitras", "proskuriakova", "ptolemy", "raman", "ramanujan", "ride",
    "montalcini", "ritchie", "robinson", "roentgen", "rosalind", "rubin", "saha", "sammet", "sanderson", "shannon",
    "shaw", "shirley", "shockley", "shtern", "sinoussi", "snyder", "solomon", "spence", "stallman", "stonebraker",
    "sutherland", "swanson", "swartz", "swirles", "taussig", "tereshkova", "tesla", "tharp", "thompson", "torvalds",
    "tu", "turing", "varahamihira", "vaughan", "visvesvaraya", "volhard", "wescoff", "wilbur", "wiles", "williams",
    "williamson", "wilson", "wing", "wozniak", "wright", "wu", "yalow", "yonath", "zhukovsky"
]


def generate_docker_like_name() -> str:
    """Generate a Docker-like random name (adjective_noun format)."""
    adjective = random.choice(DOCKER_ADJECTIVES)
    noun = random.choice(DOCKER_NOUNS)
    return f"{adjective}_{noun}"


def validate_dag_id(dag_id: str) -> bool:
    """Validate DAG ID format - must be alphanumeric with underscores and hyphens."""
    if not dag_id:
        return False
    # Allow alphanumeric characters, underscores, and hyphens
    return bool(re.match(r'^[a-zA-Z0-9_-]+$', dag_id))


def check_dag_id_uniqueness(dag_id: str) -> bool:
    """Check if DAG ID is unique by looking at existing DAGs in the database."""
    try:
        with orchestrator.status_manager as sm:
            # Get all DAGs from the database
            all_dags = sm.get_all_dags()
            existing_dag_ids = [dag.get('dag_id') for dag in all_dags]
            return dag_id not in existing_dag_ids
    except Exception as e:
        logger.error(f"Error checking DAG ID uniqueness: {e}")
        return False


def generate_unique_dag_id() -> str:
    """Generate a unique DAG ID using Docker-like naming, ensuring uniqueness."""
    max_attempts = 100
    for _ in range(max_attempts):
        dag_id = generate_docker_like_name()
        if check_dag_id_uniqueness(dag_id):
            return dag_id
    
    # If we can't generate a unique name after max_attempts, add a random suffix
    base_name = generate_docker_like_name()
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{base_name}_{suffix}"

# Pydantic models for API requests/responses
class DAGSubmissionRequest(BaseModel):
    dag_file_path: str
    dag_id: Optional[str] = None
    resume: bool = False
    fail_fast: bool = True

class DAGSubmissionResponse(BaseModel):
    dag_id: str
    execution_id: str
    status: str
    submitted_at: str
    message: str

class DAGStatusResponse(BaseModel):
    dag_id: str
    execution_id: str
    status: str
    started_at: Optional[str]
    completed_at: Optional[str]
    thread_id: Optional[str]
    tasks: List[Dict[str, Any]]

class LogEntry(BaseModel):
    task_id: str
    level: str
    message: str
    timestamp: str
    thread_id: str

class LogsResponse(BaseModel):
    dag_id: str
    execution_id: Optional[str]
    logs: List[LogEntry]
    total_count: int

class RunningDAGsResponse(BaseModel):
    running_dags: List[Dict[str, Any]]
    count: int

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global orchestrator
    import os
    # Use absolute path to ensure consistency
    db_path = os.path.abspath("maestro.db")
    orchestrator = Orchestrator(log_level="INFO", db_path=db_path)
    orchestrator.disable_rich_logging()  # Disable rich logging for server mode
    
    # Ensure database tables are created
    with orchestrator.status_manager as sm:
        # This will trigger table creation if they don't exist
        pass
    
    logger.info(f"Maestro server started with database: {db_path}")
    
    yield
    
    # Shutdown
    logger.info("Maestro server shutting down")
    if orchestrator:
        orchestrator.executor.shutdown(wait=True)

# Create FastAPI app with lifespan
app = FastAPI(
    title="Maestro API",
    description="REST API for Maestro DAG Orchestrator",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Maestro API Server", "status": "running", "timestamp": datetime.now().isoformat()}

@app.post("/dags/submit", response_model=DAGSubmissionResponse)
async def submit_dag(request: DAGSubmissionRequest, background_tasks: BackgroundTasks):
    """Submit a DAG for execution"""
    try:
        # Generate or check provided DAG ID
        if request.dag_id is not None:
            dag_id = request.dag_id.strip()
            
            # Validate DAG ID format
            if not validate_dag_id(dag_id):
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid DAG ID format: '{dag_id}'. Must contain only alphanumeric characters, underscores, and hyphens."
                )
            
            # Check uniqueness of provided DAG ID
            if not check_dag_id_uniqueness(dag_id):
                raise HTTPException(
                    status_code=400, 
                    detail=f"DAG ID '{dag_id}' already exists. Please choose a different DAG ID."
                )
        else:
            dag_id = generate_unique_dag_id()

        # Load and validate DAG
        dag = orchestrator.load_dag_from_file(request.dag_file_path, dag_id=dag_id)
        
        # Start DAG execution in background
        execution_id = orchestrator.run_dag_in_thread(
            dag=dag,
            resume=request.resume,
            fail_fast=request.fail_fast
        )
        
        return DAGSubmissionResponse(
            dag_id=dag.dag_id,
            execution_id=execution_id,
            status="submitted",
            submitted_at=datetime.now().isoformat(),
            message=f"DAG {dag.dag_id} submitted successfully"
        )
    except Exception as e:
        logger.error(f"Failed to submit DAG: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/dags/{dag_id}/status", response_model=DAGStatusResponse)
async def get_dag_status(dag_id: str, execution_id: Optional[str] = None):
    """Get status of a specific DAG execution"""
    try:
        with orchestrator.status_manager as sm:
            exec_details = sm.get_dag_execution_details(dag_id, execution_id)
            
            if not exec_details:
                raise HTTPException(status_code=404, detail=f"DAG execution not found: {dag_id}")
            
            return DAGStatusResponse(
                dag_id=dag_id,
                execution_id=exec_details["execution_id"],
                status=exec_details["status"],
                started_at=exec_details["started_at"],
                completed_at=exec_details["completed_at"],
                thread_id=exec_details.get("thread_id"),
                tasks=exec_details.get("tasks", [])
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get DAG status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dags/{dag_id}/logs", response_model=LogsResponse)
async def get_dag_logs(
    dag_id: str, 
    execution_id: Optional[str] = None,
    limit: int = 100,
    task_filter: Optional[str] = None,
    level_filter: Optional[str] = None
):
    """Get logs for a specific DAG execution"""
    try:
        with orchestrator.status_manager as sm:
            logs = sm.get_execution_logs(dag_id, execution_id, limit)
            
            # Apply filters
            if task_filter:
                logs = [log for log in logs if log["task_id"] == task_filter]
            if level_filter:
                logs = [log for log in logs if log["level"].upper() == level_filter.upper()]
            
            log_entries = [
                LogEntry(
                    task_id=log["task_id"],
                    level=log["level"],
                    message=log["message"],
                    timestamp=log["timestamp"],
                    thread_id=log["thread_id"]
                ) for log in logs
            ]
            
            return LogsResponse(
                dag_id=dag_id,
                execution_id=execution_id,
                logs=log_entries,
                total_count=len(log_entries)
            )
    except Exception as e:
        logger.error(f"Failed to get DAG logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dags/{dag_id}/logs/stream")
async def stream_dag_logs(
    dag_id: str,
    execution_id: Optional[str] = None,
    task_filter: Optional[str] = None,
    level_filter: Optional[str] = None
):
    """Stream logs for a specific DAG execution in real-time"""
    
    async def log_streamer():
        last_timestamp = None
        displayed_logs = set()
        
        while True:
            try:
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
                        log_id = f"{log['timestamp']}_{log['task_id']}_{log['level']}_{log['message'][:50]}"
                        
                        if log_id not in displayed_logs:
                            if last_timestamp is None or log["timestamp"] > last_timestamp:
                                new_logs.append(log)
                                displayed_logs.add(log_id)
                                last_timestamp = log["timestamp"]
                    
                    # Send new logs
                    for log in reversed(new_logs):
                        log_entry = LogEntry(
                            task_id=log["task_id"],
                            level=log["level"],
                            message=log["message"],
                            timestamp=log["timestamp"],
                            thread_id=log["thread_id"]
                        )
                        yield f"data: {log_entry.model_dump_json()}\n\n"
                    
                    # Clean up displayed_logs set to prevent memory issues
                    if len(displayed_logs) > 1000:
                        displayed_logs.clear()
                        last_timestamp = None
                    
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error in log streaming: {e}")
                yield f"data: {{'error': '{str(e)}'}}\n\n"
                break
    
    return StreamingResponse(
        log_streamer(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"}
    )

@app.get("/dags/running", response_model=RunningDAGsResponse)
async def get_running_dags():
    """Get all currently running DAGs"""
    try:
        with orchestrator.status_manager as sm:
            running_dags = sm.get_running_dags()
            
            return RunningDAGsResponse(
                running_dags=running_dags,
                count=len(running_dags)
            )
    except Exception as e:
        logger.error(f"Failed to get running DAGs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dags/list")
async def list_dags(status: Optional[str] = None):
    """List all DAGs with optional status filtering"""
    try:
        with orchestrator.status_manager as sm:
            if status:
                # Get DAGs with specific status
                dags = sm.get_dags_by_status(status)
                title = f"DAGs with status: {status}"
            else:
                # Get all DAGs
                dags = sm.get_all_dags()
                title = "All DAGs"
            
            return {
                "dags": dags,
                "count": len(dags),
                "title": title,
                "status_filter": status
            }
    except Exception as e:
        logger.error(f"Failed to list DAGs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/dags/{dag_id}/cancel")
async def cancel_dag(dag_id: str, execution_id: Optional[str] = None):
    """Cancel a running DAG execution"""
    try:
        with orchestrator.status_manager as sm:
            success = sm.cancel_dag_execution(dag_id, execution_id)
            
            if success:
                message = f"Successfully cancelled execution {execution_id} of DAG {dag_id}" if execution_id else f"Successfully cancelled all running executions of DAG {dag_id}"
                return {"message": message, "success": True}
            else:
                return {"message": f"No running executions found for DAG {dag_id}", "success": False}
    except Exception as e:
        logger.error(f"Failed to cancel DAG: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/dags/validate")
async def validate_dag(request: dict):
    """Validate a DAG file without executing it"""
    try:
        dag_file_path = request.get("dag_file_path")
        if not dag_file_path:
            raise ValueError("dag_file_path is required")
        
        dag = orchestrator.load_dag_from_file(dag_file_path)
        
        # Get execution order to validate dependencies
        execution_order = dag.get_execution_order()
        
        tasks_info = []
        for task_id in execution_order:
            task = dag.tasks[task_id]
            tasks_info.append({
                "task_id": task_id,
                "type": task.__class__.__name__,
                "dependencies": task.dependencies or []
            })
        
        return {
            "valid": True,
            "dag_id": dag.dag_id,
            "total_tasks": len(dag.tasks),
            "execution_order": execution_order,
            "tasks": tasks_info
        }
    except Exception as e:
        logger.error(f"Failed to validate DAG: {e}")
        return {
            "valid": False,
            "error": str(e)
        }

@app.delete("/dags/cleanup")
async def cleanup_old_executions(days: int = 30):
    """Clean up old execution records"""
    try:
        with orchestrator.status_manager as sm:
            deleted_count = sm.cleanup_old_executions(days)
            
            return {
                "message": f"Deleted {deleted_count} execution records older than {days} days",
                "deleted_count": deleted_count
            }
    except Exception as e:
        logger.error(f"Failed to cleanup executions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def start_server(host: str = "0.0.0.0", port: int = 8000, log_level: str = "info"):
    """Start the Maestro API server"""
    uvicorn.run(app, host=host, port=port, log_level=log_level)

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Maestro API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Server host")
    parser.add_argument("--port", type=int, default=8000, help="Server port")
    parser.add_argument("--log-level", default="info", help="Log level")
    
    args = parser.parse_args()
    start_server(args.host, args.port, args.log_level)

if __name__ == "__main__":
    main()
