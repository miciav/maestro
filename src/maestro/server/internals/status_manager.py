import threading
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
import re
import random
import string
import json

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func

from .models import DagORM, ExecutionORM, TaskORM, LogORM, Base

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


class StatusManager:

    # 🆕 Variabile di classe per conservare l'istanza corrente
    _instance = None

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.engine = self._get_engine()
        self.Session = self._get_session_factory()
        self._initialize_tables_once()

        # 🆕 Memorizza l'istanza globale
        StatusManager._instance = self

    def _get_engine(self):
        return create_engine(f'sqlite:///{self.db_path}')

    def _get_session_factory(self):
        return sessionmaker(bind=self.engine)

    def _initialize_tables_once(self):
        with self.engine.connect() as connection:
            Base.metadata.create_all(self.engine)

    def __enter__(self):
        self.session = self.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()

    def set_task_status(self, dag_id: str, task_id: str, status: str, execution_id: str = None):
        with self.Session.begin() as session:
            task = session.query(TaskORM).filter_by(dag_id=dag_id, id=task_id, execution_id=execution_id).first()
            if not task:
                task = TaskORM(dag_id=dag_id, id=task_id, execution_id=execution_id)
                session.add(task)
            task.status = status
            if status == "running":
                task.started_at = datetime.now()
            elif status in ["completed", "failed", "cancelled", "skipped"]:
                task.completed_at = datetime.now()

    def get_task_status(self, dag_id: str, task_id: str, execution_id: str = None) -> Optional[str]:
        with self.Session() as session:
            task = session.query(TaskORM).filter_by(
                dag_id=dag_id,
                id=task_id,
                execution_id=execution_id
            ).first()

            if not task and execution_id is not None:
                # Fall back to the task status without execution scoping when
                # a specific execution record is missing. This mirrors the
                # behaviour of earlier, file-based implementations and keeps
                # resume flows working when tasks were persisted before the
                # execution record was created.
                task = session.query(TaskORM).filter_by(
                    dag_id=dag_id,
                    id=task_id,
                    execution_id=None
                ).first()

            return task.status if task else None

    def get_dag_status(self, dag_id: str, execution_id: str = None) -> Dict[str, str]:
        with self.Session() as session:
            tasks = session.query(TaskORM).filter_by(dag_id=dag_id, execution_id=execution_id).all()
            return {task.id: task.status for task in tasks}

    def reset_dag_status(self, dag_id: str, execution_id: str = None):
        with self.Session.begin() as session:
            if execution_id:
                session.query(TaskORM).filter_by(dag_id=dag_id, execution_id=execution_id).delete()
            else:
                session.query(TaskORM).filter_by(dag_id=dag_id).delete()
                session.query(ExecutionORM).filter_by(dag_id=dag_id).delete()

    def create_dag_execution(self, dag_id: str, execution_id: str, dag_filepath: Optional[str] = None) -> str:
        with self.Session.begin() as session:
            execution = ExecutionORM(id=execution_id, dag_id=dag_id, status="running", started_at=datetime.now(), thread_id=str(threading.current_thread().ident), pid=threading.current_thread().ident)
            session.add(execution)
            dag = session.query(DagORM).filter_by(id=dag_id).first()
            if not dag:
                dag = DagORM(id=dag_id, dag_filepath=dag_filepath)
                session.add(dag)
            elif dag_filepath:
                dag.dag_filepath = dag_filepath
            return execution_id

    def create_dag_execution_with_status(self, dag_id: str, execution_id: str, status: str) -> str:
        with self.Session.begin() as session:
            execution = ExecutionORM(id=execution_id, dag_id=dag_id, status=status, pid=threading.current_thread().ident)
            if status != "created":
                execution.started_at = datetime.now()
                execution.thread_id = str(threading.current_thread().ident)
            session.add(execution)
            return execution_id

    def initialize_tasks_for_execution(self, dag_id: str, execution_id: str, task_ids: List[str]):
        with self.Session.begin() as session:
            for i, task_id in enumerate(task_ids):
                task = TaskORM(id=task_id, dag_id=dag_id, execution_id=execution_id, status="pending", insertion_order=i)
                session.add(task)

    def update_dag_execution_status(self, dag_id: str, execution_id: str, status: str):
        with self.Session.begin() as session:
            execution = session.query(ExecutionORM).filter_by(dag_id=dag_id, id=execution_id).first()
            if execution:
                # Se la DAG passa da "created" a "running" e non ha started_at, impostalo ora
                if status == "running" and execution.started_at is None:
                    execution.started_at = datetime.now()
                    execution.thread_id = str(threading.current_thread().ident)
                    execution.pid = threading.current_thread().ident

                execution.status = status

                if status in ["completed", "failed", "cancelled"]:
                    execution.completed_at = datetime.now()

    def mark_incomplete_tasks_as_failed(self, dag_id: str, execution_id: str):
        with self.Session.begin() as session:
            session.query(TaskORM).filter(
                TaskORM.dag_id == dag_id, 
                TaskORM.execution_id == execution_id, 
                TaskORM.status.in_(["running", "pending"])
            ).update(
                {TaskORM.status: "failed", TaskORM.completed_at: datetime.now()},
                synchronize_session=False
            )

    def get_running_dags(self) -> List[Dict[str, Any]]:
        with self.Session() as session:
            executions = session.query(ExecutionORM).filter_by(status="running").all()
            return [
                {
                    "dag_id": e.dag_id,
                    "execution_id": e.id,
                    "started_at": e.started_at.isoformat() if e.started_at else None,
                    "thread_id": e.thread_id,
                    "pid": e.pid
                } for e in executions
            ]

    def get_dags_by_status(self, status: str) -> List[Dict[str, Any]]:
        with self.Session() as session:
            executions = session.query(ExecutionORM).filter_by(status=status).all()
            return [
                {
                    "dag_id": e.dag_id,
                    "execution_id": e.id,
                    "started_at": e.started_at.isoformat() if e.started_at else None,
                    "completed_at": e.completed_at.isoformat() if e.completed_at else None,
                    "thread_id": e.thread_id,
                    "pid": e.pid
                } for e in executions
            ]

    def get_all_dags(self) -> List[Dict[str, Any]]:
        with self.Session() as session:
            dags = session.query(DagORM).all()
            result = []
            for dag in dags:
                latest_execution = session.query(ExecutionORM).filter_by(dag_id=dag.id).order_by(ExecutionORM.started_at.desc()).first()
                result.append({
                    "dag_id": dag.id,
                    "execution_id": latest_execution.id if latest_execution else None,
                    "status": latest_execution.status if latest_execution else "created",
                    "started_at": latest_execution.started_at.isoformat() if latest_execution and latest_execution.started_at else None,
                    "completed_at": latest_execution.completed_at.isoformat() if latest_execution and latest_execution.completed_at else None,
                    "thread_id": latest_execution.thread_id if latest_execution else None,
                    "pid": latest_execution.pid if latest_execution else None
                })
            return result

    def get_dag_summary(self) -> Dict[str, Any]:
        with self.Session() as session:
            status_counts = session.query(ExecutionORM.status, func.count(ExecutionORM.status)).group_by(ExecutionORM.status).all()
            total_count = session.query(ExecutionORM).count()
            unique_dags = session.query(DagORM).count()
            return {
                "total_executions": total_count,
                "unique_dags": unique_dags,
                "status_counts": dict(status_counts)
            }

    def get_dag_history(self, dag_id: str) -> List[Dict[str, Any]]:
        with self.Session() as session:
            executions = session.query(ExecutionORM).filter_by(dag_id=dag_id).order_by(ExecutionORM.started_at.desc()).all()
            return [
                {
                    "execution_id": e.id,
                    "status": e.status,
                    "started_at": e.started_at.isoformat() if e.started_at else None,
                    "completed_at": e.completed_at.isoformat() if e.completed_at else None,
                    "thread_id": e.thread_id,
                    "pid": e.pid
                } for e in executions
            ]

    def cleanup_old_executions(self, days_to_keep: int = 30):
        with self.Session.begin() as session:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            executions_to_delete = session.query(ExecutionORM).filter(ExecutionORM.started_at < cutoff_date).all()
            for execution in executions_to_delete:
                session.delete(execution)
            return len(executions_to_delete)

    def cancel_dag_execution(self,
                             dag_id: str,
                             execution_id: str = None) -> bool:
        with self.Session.begin() as session:
            query = session.query(ExecutionORM).filter_by(dag_id=dag_id, status="running")
            if execution_id:
                query = query.filter_by(id=execution_id)
            execution = query.first()
            if execution:
                # Mark the execution as cancelled
                execution.status = "cancelled"
                execution.completed_at = datetime.now()
                
                # Mark all incomplete tasks (running, pending) as cancelled
                session.query(TaskORM).filter(
                    TaskORM.dag_id == dag_id,
                    TaskORM.execution_id == execution.id,
                    TaskORM.status.in_(["running", "pending"])
                ).update(
                    {TaskORM.status: "cancelled", TaskORM.completed_at: datetime.now()},
                    synchronize_session=False
                )
                
                return True
            return False

    def get_dag_execution_details(self, dag_id: str, execution_id: str = None) -> Dict[str, Any]:
        with self.Session() as session:
            query = session.query(ExecutionORM).filter_by(dag_id=dag_id)
            if execution_id:
                query = query.filter_by(id=execution_id)
            else:
                query = query.order_by(ExecutionORM.started_at.desc())
            execution = query.first()

            if not execution:
                return {}

            tasks = session.query(TaskORM).filter_by(dag_id=dag_id, execution_id=execution.id).order_by(TaskORM.insertion_order).all()
            return {
                "execution_id": execution.id,
                "status": execution.status,
                "started_at": execution.started_at.isoformat() if execution.started_at else None,
                "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
                "thread_id": execution.thread_id,
                "pid": execution.pid,
                "tasks": [
                    {
                        "task_id": task.id,
                        "status": task.status,
                        "started_at": task.started_at.isoformat() if task.started_at else None,
                        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                        "thread_id": task.thread_id
                    } for task in tasks
                ]
            }

    def log_message(self, dag_id: str, execution_id: str, task_id: str, level: str, message: str):
        with self.Session.begin() as session:
            log = LogORM(dag_id=dag_id, execution_id=execution_id, task_id=task_id, level=level, message=message, thread_id=str(threading.current_thread().ident))
            session.add(log)

    # 🆕 Metodo helper per aggiungere log in modo più comodo
    def add_log(self, dag_id: str, execution_id: str, task_id: str, message: str, level: str = "INFO"):
        self.log_message(
            dag_id=dag_id,
            execution_id=execution_id,
            task_id=task_id,
            level=level,
            message=message
        )

    def get_execution_logs(self, dag_id: str, execution_id: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        with self.Session() as session:
            query = session.query(LogORM).filter_by(dag_id=dag_id)
            if execution_id:
                query = query.filter_by(execution_id=execution_id)
            logs = query.order_by(LogORM.timestamp.desc()).limit(limit).all()
            return [
                {
                    "task_id": log.task_id,
                    "level": log.level,
                    "message": log.message,
                    "timestamp": log.timestamp.isoformat(),
                    "thread_id": log.thread_id
                } for log in logs
            ]

    def validate_dag_id(self, dag_id: str) -> bool:
        if not dag_id:
            return False
        return bool(re.match(r'^[a-zA-Z0-9_-]+$', dag_id))

    def check_dag_id_uniqueness(self, dag_id: str) -> bool:
        with self.Session() as session:
            return session.query(DagORM).filter_by(id=dag_id).first() is None

    def generate_unique_dag_id(self) -> str:
        max_attempts = 100
        for _ in range(max_attempts):
            dag_id = f"{random.choice(DOCKER_ADJECTIVES)}_{random.choice(DOCKER_NOUNS)}"
            if self.check_dag_id_uniqueness(dag_id):
                return dag_id
        base_name = f"{random.choice(DOCKER_ADJECTIVES)}_{random.choice(DOCKER_NOUNS)}"
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"{base_name}_{suffix}"

    def get_latest_execution(self, dag_id: str) -> Optional[Dict[str, Any]]:
        with self.Session() as session:
            execution = session.query(ExecutionORM).filter_by(dag_id=dag_id).order_by(ExecutionORM.started_at.desc()).first()
            if execution:
                return {
                    "execution_id": execution.id,
                    "status": execution.status,
                    "started_at": execution.started_at.isoformat() if execution.started_at else None,
                    "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
                    "thread_id": execution.thread_id,
                    "pid": execution.pid
                }
            return None

    def save_dag_definition(self, dag, dag_filepath: Optional[str] = None):
        with self.Session.begin() as session:
            dag_orm = session.query(DagORM).filter_by(id=dag.dag_id).first()
            if not dag_orm:
                dag_orm = DagORM(id=dag.dag_id)
                session.add(dag_orm)
            dag_orm.definition = json.dumps(dag.to_dict())
            if dag_filepath:
                dag_orm.dag_filepath = dag_filepath

    def get_dag_definition(self, dag_id: str) -> Optional[Dict]:
        with self.Session() as session:
            dag = session.query(DagORM).filter_by(id=dag_id).first()
            if not dag:
                return None

            if dag.definition is None:
                return None

            try:
                return json.loads(dag.definition)
            except (TypeError, json.JSONDecodeError):
                # If the stored definition is not valid JSON, fall back to returning the raw value
                return dag.definition

    def delete_dag(self, dag_id: str) -> int:
        with self.Session.begin() as session:
            dag = session.query(DagORM).filter_by(id=dag_id).first()
            if dag:
                session.delete(dag)
                return 1
            return 0


    # 🆕 Metodo per recuperare l'istanza globale
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            raise RuntimeError("StatusManager has not been initialized yet.")
        return cls._instance
