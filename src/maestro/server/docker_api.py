
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import asyncio
import json
import uuid

from maestro.core.orchestrator import Orchestrator

# Module-level orchestrator variable that will be set by the app
orchestrator = None


router = APIRouter(
    prefix="/v1",
    tags=["Docker-Inspired API"],
)

# --- Models ---

class DAGCreateRequest(BaseModel):
    dag_file_path: str
    dag_id: Optional[str] = None

class DAGCreateResponse(BaseModel):
    dag_id: str
    message: str

class DAGRunRequest(BaseModel):
    dag_id: str
    resume: bool = False
    fail_fast: bool = True

class DAGRunResponse(BaseModel):
    dag_id: str
    execution_id: str
    status: str
    message: str

class DAGRemoveResponse(BaseModel):
    dag_id: str
    message: str
    
class DAGStopResponse(BaseModel):
    dag_id: str
    message: str

class DAGResumeResponse(BaseModel):
    dag_id: str
    message: str

# --- Endpoints ---

@router.post("/dags/create", response_model=DAGCreateResponse)
async def create_dag(request: DAGCreateRequest):
    """
    Creates a new DAG from a YAML file and stores its definition.
    """
    try:
        # Generate or check provided DAG ID
        print(f"[DEBUG] create_dag: Received dag_file_path: {request.dag_file_path}")
        if request.dag_id is not None:
            dag_id = request.dag_id.strip()
            
            # Validate DAG ID format
            print(f"[DEBUG] create_dag: dag_id = {dag_id}")
            with orchestrator.status_manager as sm:
                is_valid = sm.validate_dag_id(dag_id)
                print(f"[DEBUG] create_dag: is_valid = {is_valid}")
                if not is_valid:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Invalid DAG ID format: '{dag_id}'. Must contain only alphanumeric characters, underscores, and hyphens."
                    )
                
                is_unique = sm.check_dag_id_uniqueness(dag_id)
                print(f"[DEBUG] create_dag: is_unique = {is_unique}")
                if not is_unique:
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
            sm.save_dag_definition(dag)
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

@router.post("/dags/{dag_id}/run", response_model=DAGRunResponse)
async def run_dag(dag_id: str, request: DAGRunRequest, background_tasks: BackgroundTasks):
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

@router.get("/dags/{dag_id}/log")
async def log_dag(dag_id: str, execution_id: Optional[str] = None, limit: int = 100, task_filter: Optional[str] = None, level_filter: Optional[str] = None):
    """
    Gets the logs of a DAG execution.
    """
    try:
        with orchestrator.status_manager as sm:
            logs = sm.get_execution_logs(dag_id, execution_id, limit)
            
            # Apply filters
            if task_filter:
                logs = [log for log in logs if log["task_id"] == task_filter]
            if level_filter:
                logs = [log for log in logs if log["level"].upper() == level_filter.upper()]
            
            return logs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dags/{dag_id}/attach")
async def attach_dag(dag_id: str, execution_id: Optional[str] = None, task_filter: Optional[str] = None, level_filter: Optional[str] = None):
    """
    Attaches to the live log stream of a DAG execution.
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
                    
                    # Send new logs
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

@router.delete("/dags/{dag_id}", response_model=DAGRemoveResponse)
async def rm_dag(dag_id: str, force: bool = False):
    """
    Removes a DAG and its executions. 
    With `force=True`, it will also remove running DAGs.
    """
    try:
        with orchestrator.status_manager as sm:
            if not force:
                running_dags = sm.get_running_dags()
                if any(dag["dag_id"] == dag_id for dag in running_dags):
                    raise HTTPException(status_code=409, detail=f"DAG '{dag_id}' is currently running. Use --force to remove it.")
            
            deleted_count = sm.delete_dag(dag_id)

            if deleted_count > 0:
                return DAGRemoveResponse(dag_id=dag_id, message=f"DAG '{dag_id}' and its executions removed successfully.")
            else:
                raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dags", response_model=List[Dict[str, Any]])
async def ls_dags(status: Optional[str] = Query(None, description="Filter by status (active, terminated, all)")):
    """
    Lists all DAGs, with optional filtering.
    - `active`: shows running DAGs.
    - `terminated`: shows completed, failed, and cancelled DAGs.
    - `all`: shows all DAGs.
    """
    try:
        with orchestrator.status_manager as sm:
            if status == "active":
                dags = sm.get_dags_by_status("running")
            elif status == "terminated":
                dags = sm.get_dags_by_status("completed")
                dags.extend(sm.get_dags_by_status("failed"))
                dags.extend(sm.get_dags_by_status("cancelled"))
            else: # all
                dags = sm.get_all_dags()
            
            return dags
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/dags/{dag_id}/stop", response_model=DAGStopResponse)
async def stop_dag(dag_id: str, execution_id: Optional[str] = None):
    """
    Stops a running DAG execution.
    """
    try:
        with orchestrator.status_manager as sm:
            success = sm.cancel_dag_execution(dag_id, execution_id)
            
            if success:
                message = f"Successfully stopped execution {execution_id} of DAG {dag_id}" if execution_id else f"Successfully stopped all running executions of DAG {dag_id}"
                return DAGStopResponse(dag_id=dag_id, message=message)
            else:
                return DAGStopResponse(dag_id=dag_id, message=f"No running executions found for DAG {dag_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/dags/{dag_id}/resume", response_model=DAGResumeResponse)
async def resume_dag(dag_id: str, execution_id: str, background_tasks: BackgroundTasks):
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
            fail_fast=True # Defaulting to fail_fast on resume
        )

        return DAGResumeResponse(
            dag_id=dag.dag_id,
            message=f"DAG '{dag.dag_id}' resumed with new execution ID: {new_execution_id}"
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
