import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Dict, Optional

# Core components imports
from src.generator import EcommerceDataGenerator
from src.validator import DataQualityAuditor
from src.transformer import DataTransformer
from src.database import EcommerceDatabase
from src.analytics import EcommerceAnalytics

class TaskState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"
    SKIPPED = "SKIPPED"

@dataclass
class Task:
    name: str
    fn: Callable[[], None]
    dependencies: List[str] = field(default_factory=list)
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    max_retries: int = 2
    retry_delay: float = 2.0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None

class Orchestrator:
    """
    Native Python DAG Task Graph Orchestrator for Windows + Python 3.13.
    Manages task graph dependencies, retries, upstream failure halting, and state persistence.
    """
    def __init__(self, state_file_path: str = os.path.join("data", "processed", "orchestrator_state.json")):
        self.tasks: Dict[str, Task] = {}
        self.execution_order: List[str] = []
        self.state_file_path = state_file_path

    def add_task(self, name: str, fn: Callable[[], None], dependencies: Optional[List[str]] = None, max_retries: int = 2, retry_delay: float = 2.0) -> None:
        """Registers a task in the DAG."""
        if name in self.tasks:
            raise ValueError(f"Task '{name}' is already registered in the orchestrator.")
        deps = dependencies or []
        self.tasks[name] = Task(
            name=name,
            fn=fn,
            dependencies=deps,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        self.execution_order.append(name)

    def _check_upstream_passed(self, task: Task) -> bool:
        """Verifies if all upstream dependency tasks succeeded."""
        for dep in task.dependencies:
            dep_task = self.tasks.get(dep)
            if not dep_task or dep_task.state != TaskState.SUCCESS:
                return False
        return True

    def run_dag(self) -> dict:
        """
        Executes registered tasks in the DAG topological order.
        Manages automatic retries and propagates UPSTREAM_FAILED status.
        """
        print("[ORCHESTRATOR] Starting Native Local DAG Task Graph Execution...")
        start_time = time.time()

        for name in self.execution_order:
            task = self.tasks[name]

            # Check if upstream dependencies passed
            if not self._check_upstream_passed(task):
                task.state = TaskState.UPSTREAM_FAILED
                print(f" [TASK] {task.name:25s} -> {task.state.value} (Upstream dependency failed)")
                continue

            # Execute task with retries
            task.state = TaskState.RUNNING
            print(f" [TASK] {task.name:25s} -> RUNNING...")

            success = False
            while task.attempts <= task.max_retries and not success:
                task.attempts += 1
                t0 = time.time()
                try:
                    task.fn()
                    task.duration_seconds = round(time.time() - t0, 3)
                    task.state = TaskState.SUCCESS
                    task.error_message = None
                    success = True
                    print(f" [TASK] {task.name:25s} -> SUCCESS (Attempt {task.attempts}, Duration: {task.duration_seconds}s)")

                except Exception as e:
                    task.duration_seconds = round(time.time() - t0, 3)
                    task.error_message = str(e)
                    if task.attempts <= task.max_retries:
                        print(f"  [RETRY] {task.name} failed (Attempt {task.attempts}/{task.max_retries + 1}): {e}. Retrying in {task.retry_delay}s...")
                        time.sleep(task.retry_delay)
                    else:
                        task.state = TaskState.FAILED
                        print(f"  [FAILED] {task.name} failed permanently after {task.attempts} attempts: {e}")


        total_duration = round(time.time() - start_time, 3)
        self.persist_state(total_duration=total_duration)
        return self.get_summary()

    def persist_state(self, total_duration: float = 0.0) -> None:
        """Persists task execution metadata to orchestrator_state.json."""
        os.makedirs(os.path.dirname(self.state_file_path), exist_ok=True)
        state_data = {
            "total_duration_seconds": total_duration,
            "tasks": {}
        }
        for name, task in self.tasks.items():
            state_data["tasks"][name] = {
                "name": task.name,
                "state": task.state.value,
                "attempts": task.attempts,
                "max_retries": task.max_retries,
                "duration_seconds": task.duration_seconds,
                "dependencies": task.dependencies,
                "error_message": task.error_message
            }
        with open(self.state_file_path, "w", encoding="utf-8") as f:
            json.dump(state_data, f, indent=2)

    def get_summary(self) -> dict:
        """Returns DAG execution summary."""
        states_count = {state.value: 0 for state in TaskState}
        for task in self.tasks.values():
            states_count[task.state.value] += 1
        return {
            "total_tasks": len(self.tasks),
            "state_counts": states_count,
            "passed": states_count[TaskState.SUCCESS.value] == len(self.tasks)
        }

def build_ecommerce_dag(
    raw_dir: str = os.path.join("data", "raw"),
    processed_dir: str = os.path.join("data", "processed"),
    db_path: str = os.path.join("data", "ecommerce.db")
) -> Orchestrator:
    """
    Constructs the 7-task E-Commerce Platform DAG linking existing components.
    Does not duplicate business logic.
    """
    orch = Orchestrator(state_file_path=os.path.join(processed_dir, "orchestrator_state.json"))

    # Task 1: Generate Raw Data
    def task_gen():
        generator = EcommerceDataGenerator(seed=42)
        cust = generator.generate_customers(100)
        prod = generator.generate_products(30)
        ord_ = generator.generate_orders(1000, cust, prod)
        pay = generator.generate_payments(ord_)
        rev = generator.generate_reviews(300, cust, prod)
        generator.save_to_csv(cust, os.path.join(raw_dir, "customers.csv"))
        generator.save_to_csv(prod, os.path.join(raw_dir, "products.csv"))
        generator.save_to_csv(ord_, os.path.join(raw_dir, "orders.csv"))
        generator.save_to_csv(pay, os.path.join(raw_dir, "payments.csv"))
        generator.save_to_csv(rev, os.path.join(raw_dir, "reviews.csv"))

    # Task 2: Validate Raw Data
    def task_val():
        auditor = DataQualityAuditor(raw_data_dir=raw_dir)
        report_path = os.path.join(processed_dir, "data_quality_report.json")
        report = auditor.audit_datasets(report_output_path=report_path)
        if not report["summary"]["overall_passed"]:
            raise ValueError(f"Data Quality Audit Failed with {report['summary']['total_issues_found']} issues.")

    # Task 3: Transform Data
    def task_trans():
        transformer = DataTransformer(raw_data_dir=raw_dir, processed_data_dir=processed_dir)
        transformer.transform_datasets()

    # Task 4: Load SQLite DB
    def task_load():
        db = EcommerceDatabase(db_path=db_path)
        verification = db.load_csv_data(raw_data_dir=processed_dir)
        for table, meta in verification.items():
            if not meta["matched"]:
                raise ValueError(f"Row count mismatch in table '{table}': CSV={meta['csv_count']} vs DB={meta['db_count']}")

    # Task 5: Verify Database Foreign Keys
    def task_verify():
        db = EcommerceDatabase(db_path=db_path)
        violations = db.verify_foreign_keys()
        if violations:
            raise ValueError(f"SQLite PRAGMA foreign_key_check failed: {violations}")

    # Task 6: Run SQL Analytics
    def task_analytics():
        analytics = EcommerceAnalytics(db_path=db_path)
        kpis = analytics.get_executive_kpis()
        reconciliation = analytics.reconcile_revenue()
        if len(reconciliation) > 0:
            raise ValueError(f"Revenue reconciliation detected {len(reconciliation)} discrepancies!")

    # Task 7: Export Analytics Reports
    def task_export():
        analytics = EcommerceAnalytics(db_path=db_path)
        summary = {
            "kpis": analytics.get_executive_kpis().to_dict(orient="records")[0],
            "category_revenue": analytics.get_revenue_by_category().to_dict(orient="records"),
            "sentiment": analytics.get_rating_category_distribution().to_dict(orient="records")
        }
        out_file = os.path.join(processed_dir, "analytics_summary.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    # Register Tasks & Dependencies
    orch.add_task("generate_raw_data", task_gen)
    orch.add_task("validate_raw_data", task_val, dependencies=["generate_raw_data"])
    orch.add_task("transform_data", task_trans, dependencies=["validate_raw_data"])
    orch.add_task("load_sqlite_db", task_load, dependencies=["transform_data"])
    orch.add_task("verify_database", task_verify, dependencies=["load_sqlite_db"])
    orch.add_task("run_sql_analytics", task_analytics, dependencies=["verify_database"])
    orch.add_task("export_analytics_reports", task_export, dependencies=["run_sql_analytics"])

    return orch
