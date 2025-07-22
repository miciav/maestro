from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
import asyncio
import json
import logging

from maestro.server.internals.orchestrator import Orchestrator

# Configure logging
logger = logging.getLogger(__name__)

# Router for log-related endpoints
router = APIRouter(
    prefix="/logs",
    tags=["Logs"],
)


# --- Pydantic Models ---

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


# --- Helper function to get orchestrator ---

def get_orchestrator():
    """Get the orchestrator instance from the app context"""
    from maestro.server.app import app
    if not hasattr(app.state, 'orchestrator'):
        raise HTTPException(status_code=500, detail="Server not properly initialized")
    return app.state.orchestrator


# --- Endpoints ---

@router.get("/{dag_id}", response_model=LogsResponse)
async def get_dag_logs(
    dag_id: str, 
    execution_id: Optional[str] = None,
    limit: int = Query(100, description="Maximum number of logs to return"),
    task_filter: Optional[str] = Query(None, description="Filter logs by task ID"),
    level_filter: Optional[str] = Query(None, description="Filter logs by level (INFO, WARNING, ERROR)"),
    orchestrator: Orchestrator = Depends(get_orchestrator)
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


@router.get("/{dag_id}/stream")
async def stream_dag_logs(
    dag_id: str,
    execution_id: Optional[str] = None,
    task_filter: Optional[str] = Query(None, description="Filter logs by task ID"),
    level_filter: Optional[str] = Query(None, description="Filter logs by level (INFO, WARNING, ERROR)"),
    orchestrator: Orchestrator = Depends(get_orchestrator)
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


@router.get("/{dag_id}/attach")
async def attach_dag_logs(
    dag_id: str, 
    execution_id: Optional[str] = None, 
    task_filter: Optional[str] = Query(None, description="Filter logs by task ID"),
    level_filter: Optional[str] = Query(None, description="Filter logs by level (INFO, WARNING, ERROR)"),
    orchestrator: Orchestrator = Depends(get_orchestrator)
):
    """
    Attaches to the live log stream of a DAG execution (alias for stream).
    Compatible with Docker-style attach API.
    """
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
                    
                    # Send new logs in simple JSON format
                    for log in reversed(new_logs):
                        yield f"data: {json.dumps(log)}\n\n"
                    
                    # Clean up displayed_logs set to prevent memory issues
                    if len(displayed_logs) > 1000:
                        displayed_logs.clear()
                        last_timestamp = None
                    
                    await asyncio.sleep(1)
                    
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
                break
    
    return StreamingResponse(
        log_streamer(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"}
    )
