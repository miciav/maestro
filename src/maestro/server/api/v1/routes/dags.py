from fastapi import APIRouter, HTTPException, BackgroundTasks, Query, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import asyncio
import json
import uuid
from datetime import datetime

from maestro.shared.dag import DAG
from maestro.server.internals.orchestrator import Orchestrator

# Router for DAG-related endpoints
router = APIRouter(
    prefix="/dags",
    tags=["DAGs"],
)


# --- Pydantic Models ---

class DAGCreateRequest(BaseModel):
    dag_file_path: str
    dag_id: Optional[str] = None


class DAGCreateResponse(BaseModel):
    dag_id: str
    message: str


class DAGRunRequest(BaseModel):
    resume: bool = False
    fail_fast: bool = True


class DAGRunResponse(BaseModel):
    dag_id: str
    execution_id: str
    status: str
    message: str


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


class DAGRemoveResponse(BaseModel):
    dag_id: str
    message: str


class DAGStopResponse(BaseModel):
    dag_id: str
    message: str


class DAGResumeResponse(BaseModel):
    dag_id: str
    message: str


class RunningDAGsResponse(BaseModel):
    running_dags: List[Dict[str, Any]]
    count: int


# --- Helper function to get orchestrator ---

def get_orchestrator():
    """Get the orchestrator instance from the app context"""
    from maestro.server.app import app
    if not hasattr(app.state, 'orchestrator'):
        raise HTTPException(status_code=500, detail="Server not properly initialized")
    return app.state.orchestrator


# --- Endpoints ---

@router.post("/create", response_model=DAGCreateResponse)
async def create_dag(request: DAGCreateRequest, orchestrator: Orchestrator = Depends(get_orchestrator)):
    """
    Creates a new DAG from a YAML file and stores its definition.
    """
    try:
        # Generate or check provided DAG ID
        if request.dag_id is not None:
            dag_id = request.dag_id.strip()
            
            # Validate DAG ID format
            with orchestrator.status_manager as sm:
                if not sm.validate_dag_id(dag_id):
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Invalid DAG ID format: '{dag_id}'. Must contain only alphanumeric characters, underscores, and hyphens."
                    )
                
                if not sm.check_dag_id_uniqueness(dag_id):
                    raise HTTPException(
                        status_code=400, 
                        detail=f"DAG ID '{dag_id}' already exists. Please choose a different DAG ID."
                    )
        else:
            with orchestrator.status_manager as sm:
                dag_id = sm.generate_unique_dag_id()

        # Load and validate DAG
        dag = orchestrator.load_dag_from_file(request.dag_file_path, dag_id=dag_id)
        
        # Create a new execution ID
        execution_id = str(uuid.uuid4())
        
        with orchestrator.status_manager as sm:
            sm.save_dag_definition(dag, request.dag_file_path)
            # Create initial execution with 'created' status
            sm.create_dag_execution_with_status(dag_id, execution_id, "created")
            # Initialize all tasks with pending status
            task_ids = list(dag.tasks.keys())
            sm.initialize_tasks_for_execution(dag_id, execution_id, task_ids)

        return DAGCreateResponse(
            dag_id=dag.dag_id,
            message=f"DAG '{dag.dag_id}' created successfully."
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{dag_id}/run", response_model=DAGRunResponse)
async def run_dag(dag_id: str, request: DAGRunRequest, background_tasks: BackgroundTasks, orchestrator: Orchestrator = Depends(get_orchestrator)):
    """
    Runs a previously created DAG.
    """
    try:
        with orchestrator.status_manager as sm:
            dag_definition = sm.get_dag_definition(dag_id)
            if not dag_definition:
                raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found.")
            
            # Get the latest execution for this DAG
            latest_execution = sm.get_latest_execution(dag_id)
            
            if latest_execution and latest_execution["status"] == "created":
                # Use the existing execution ID from the created DAG
                execution_id = latest_execution["execution_id"]
            else:
                # Create a new execution ID for subsequent runs
                execution_id = str(uuid.uuid4())

        dag = orchestrator.dag_loader.load_dag_from_dict(dag_definition)
        
        # Run the DAG with the determined execution ID
        orchestrator.run_dag_in_thread(
            dag=dag,
            execution_id=execution_id,
            resume=request.resume,
            fail_fast=request.fail_fast
        )

        return DAGRunResponse(
            dag_id=dag.dag_id,
            execution_id=execution_id,
            status="submitted",
            message=f"DAG '{dag.dag_id}' submitted for execution."
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{dag_id}/status", response_model=DAGStatusResponse)
async def get_dag_status(dag_id: str, execution_id: Optional[str] = None, orchestrator: Orchestrator = Depends(get_orchestrator)):
    """Get status of a specific DAG execution"""
    try:
        with orchestrator.status_manager as sm:
            exec_details: dict[str, Any] = sm.get_dag_execution_details(dag_id, execution_id)
            
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
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[Dict[str, Any]])
async def list_dags(status: Optional[str] = Query(None, description="Filter by status"), orchestrator: Orchestrator = Depends(get_orchestrator)):
    """
    Lists all DAGs, with optional filtering.
    - `active` or `running`: shows running DAGs.
    - `terminated`: shows completed, failed, and cancelled DAGs.
    - specific status: shows DAGs with that status
    - `all` or no filter: shows all DAGs.
    """
    try:
        with orchestrator.status_manager as sm:
            if status in ["active", "running"]:
                dags = sm.get_dags_by_status("running")
            elif status == "terminated":
                dags = sm.get_dags_by_status("completed")
                dags.extend(sm.get_dags_by_status("failed"))
                dags.extend(sm.get_dags_by_status("cancelled"))
            elif status and status != "all":
                dags = sm.get_dags_by_status(status)
            else:  # all or no filter
                dags = sm.get_all_dags()
            
            return dags
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/running", response_model=RunningDAGsResponse)
async def get_running_dags(orchestrator: Orchestrator = Depends(get_orchestrator)):
    """Get all currently running DAGs"""
    try:
        with orchestrator.status_manager as sm:
            running_dags = sm.get_running_dags()
            
            return RunningDAGsResponse(
                running_dags=running_dags,
                count=len(running_dags)
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{dag_id}/cancel")
async def cancel_dag(dag_id: str, execution_id: Optional[str] = None, orchestrator: Orchestrator = Depends(get_orchestrator)):
    """Cancel a running DAG execution"""
    try:
        # Use the orchestrator's cancel method which properly signals the thread
        success = orchestrator.cancel_dag_execution(dag_id, execution_id)
        
        if success:
            message = f"Successfully cancelled execution {execution_id} of DAG {dag_id}" if execution_id else f"Successfully cancelled all running executions of DAG {dag_id}"
            return {"message": message, "success": True}
        else:
            return {"message": f"No running executions found for DAG {dag_id}", "success": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{dag_id}/stop", response_model=DAGStopResponse)
async def stop_dag(dag_id: str, execution_id: Optional[str] = None, orchestrator: Orchestrator = Depends(get_orchestrator)):
    """
    Stops a running DAG execution (alias for cancel).
    """
    try:
        success = orchestrator.cancel_dag_execution(dag_id, execution_id)
        
        if success:
            message = f"Successfully stopped execution {execution_id} of DAG {dag_id}" if execution_id else f"Successfully stopped all running executions of DAG {dag_id}"
            return DAGStopResponse(dag_id=dag_id, message=message)
        else:
            return DAGStopResponse(dag_id=dag_id, message=f"No running executions found for DAG {dag_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{dag_id}/resume", response_model=DAGResumeResponse)
async def resume_dag(dag_id: str, execution_id: str, background_tasks: BackgroundTasks, orchestrator: Orchestrator = Depends(get_orchestrator)):
    """
    Resumes a previously stopped DAG execution.
    """
    try:
        with orchestrator.status_manager as sm:
            dag_definition = sm.get_dag_definition(dag_id)
            if not dag_definition:
                raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found.")

        dag = orchestrator.dag_loader.load_dag_from_dict(dag_definition)

        new_execution_id = orchestrator.run_dag_in_thread(
            dag=dag,
            resume=True,
            fail_fast=True  # Defaulting to fail_fast on resume
        )

        return DAGResumeResponse(
            dag_id=dag.dag_id,
            message=f"DAG '{dag.dag_id}' resumed with new execution ID: {new_execution_id}"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{dag_id}", response_model=DAGRemoveResponse)
async def remove_dag(dag_id: str, force: bool = False, orchestrator: Orchestrator = Depends(get_orchestrator)):
    """
    Removes a DAG and its executions. 
    With `force=True`, it will also remove running DAGs.
    """
    try:
        with orchestrator.status_manager as sm:
            if not force:
                running_dags = sm.get_running_dags()
                if any(dag["dag_id"] == dag_id for dag in running_dags):
                    raise HTTPException(status_code=409, detail=f"DAG '{dag_id}' is currently running. Use force=true to remove it.")
            
            deleted_count = sm.delete_dag(dag_id)

            if deleted_count > 0:
                return DAGRemoveResponse(dag_id=dag_id, message=f"DAG '{dag_id}' and its executions removed successfully.")
            else:
                raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate")
async def validate_dag(request: dict, orchestrator: Orchestrator = Depends(get_orchestrator)):
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
        return {
            "valid": False,
            "error": str(e)
        }


@router.delete("/cleanup")
async def cleanup_old_executions(days: int = 30, orchestrator: Orchestrator = Depends(get_orchestrator)):
    """Clean up old execution records"""
    try:
        with orchestrator.status_manager as sm:
            deleted_count = sm.cleanup_old_executions(days)
            
            return {
                "message": f"Deleted {deleted_count} execution records older than {days} days",
                "deleted_count": deleted_count
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
