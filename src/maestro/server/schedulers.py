
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
import logging

logger = logging.getLogger(__name__)

def create_scheduler(db_uri: str) -> BackgroundScheduler:
    """Creates and configures a BackgroundScheduler with a SQLAlchemy job store."""
    jobstores = {
        'default': SQLAlchemyJobStore(url=db_uri)
    }
    executors = {
        'default': ThreadPoolExecutor(10),  # Pool of 10 threads for jobs
    }
    job_defaults = {
        'coalesce': True,  # Run once if multiple runs were missed
        'max_instances': 1,  # Only one instance of a job can run at a time
        'misfire_grace_time': 600  # 10 minutes grace time for missed jobs
    }

    scheduler = BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        job_defaults=job_defaults,
        timezone="UTC"  # Use UTC for consistency
    )

    logger.info("Scheduler created with SQLAlchemyJobStore.")
    return scheduler

