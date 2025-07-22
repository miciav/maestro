
from apscheduler.schedulers.background import BackgroundScheduler
from maestro.server.schedulers import create_scheduler
from maestro.server.internals.orchestrator import Orchestrator
from maestro.shared.dag import DAG
import logging

logger = logging.getLogger(__name__)

class SchedulerService:
    def __init__(self, orchestrator: Orchestrator, db_uri: str):
        self.orchestrator = orchestrator
        self.scheduler: BackgroundScheduler = create_scheduler(db_uri)

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("SchedulerService started.")

    def shutdown(self):
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("SchedulerService shut down.")

    def schedule_dag(self, dag: DAG, paused: bool = False):
        if not dag.cron_schedule:
            logger.warning(f"DAG {dag.dag_id} has no cron schedule. Cannot schedule.")
            return

        job_id = f"dag:{dag.dag_id}"
        self.scheduler.add_job(
            self.orchestrator.execute_scheduled_dag,
            trigger='cron',
            id=job_id,
            name=dag.dag_id,
            args=[dag.dag_id],
            replace_existing=True,
            **dag.cron_schedule_to_aps_kwargs()
        )
        if paused:
            self.scheduler.pause_job(job_id)
            logger.info(f"DAG {dag.dag_id} scheduled and paused with cron: {dag.cron_schedule}")
        else:
            logger.info(f"DAG {dag.dag_id} scheduled with cron: {dag.cron_schedule}")

    def unschedule_dag(self, dag_id: str):
        job_id = f"dag:{dag_id}"
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"DAG {dag_id} unscheduled.")
        except JobLookupError:
            logger.warning(f"Job for DAG {dag_id} not found in scheduler.")

    def resume_dag_schedule(self, dag_id: str):
        job_id = f"dag:{dag_id}"
        self.scheduler.resume_job(job_id)
        logger.info(f"Resumed schedule for DAG {dag_id}")

    def pause_dag_schedule(self, dag_id: str):
        job_id = f"dag:{dag_id}"
        self.scheduler.pause_job(job_id)
        logger.info(f"Paused schedule for DAG {dag_id}")

