from fastapi import APIRouter
from .routes import dags, logs, health

# Create the main v1 router
api_router = APIRouter(prefix="/v1")

# Include all route modules
api_router.include_router(dags.router)
api_router.include_router(logs.router)
api_router.include_router(health.router)
