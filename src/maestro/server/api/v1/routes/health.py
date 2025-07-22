from fastapi import APIRouter
from datetime import datetime

# Router for health check endpoints
router = APIRouter(
    tags=["Health"],
)


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Maestro API Server",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }


@router.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    return {
        "status": "ready",
        "timestamp": datetime.now().isoformat()
    }
