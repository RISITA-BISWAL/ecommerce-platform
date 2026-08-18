import json
import os
import pytest
from src.orchestrator import Orchestrator, TaskState, build_ecommerce_dag

@pytest.fixture
def orch_env(tmp_path):
    """Sets up temporary paths for orchestrator testing."""
    raw = os.path.join(tmp_path, "raw")
    proc = os.path.join(tmp_path, "processed")
    db_file = os.path.join(tmp_path, "ecommerce.db")
    state_file = os.path.join(proc, "orchestrator_state.json")
    return raw, proc, db_file, state_file

def test_orchestrator_successful_dag_execution(orch_env):
    """Test 1: Complete successful DAG execution across all 7 tasks."""
    raw, proc, db_file, state_file = orch_env
    orch = build_ecommerce_dag(raw_dir=raw, processed_dir=proc, db_path=db_file)
    
    summary = orch.run_dag()
    assert summary["passed"] is True
    assert summary["total_tasks"] == 7
    assert summary["state_counts"][TaskState.SUCCESS.value] == 7
    assert os.path.exists(state_file)

def test_orchestrator_execution_order(orch_env):
    """Test 2: Verify topological execution order of tasks."""
    raw, proc, db_file, _ = orch_env
    orch = build_ecommerce_dag(raw_dir=raw, processed_dir=proc, db_path=db_file)
    
    expected_order = [
        "generate_raw_data",
        "validate_raw_data",
        "transform_data",
        "load_sqlite_db",
        "verify_database",
        "run_sql_analytics",
        "export_analytics_reports"
    ]
    assert orch.execution_order == expected_order

def test_orchestrator_validation_failure_halts_downstream(tmp_path):
    """
    Test 3 & 6: Validation failure prevents downstream tasks and sets UPSTREAM_FAILED.
    """
    state_file = os.path.join(tmp_path, "state.json")
    orch = Orchestrator(state_file_path=state_file)
    
    execution_history = []
    
    def task_1():
        execution_history.append("t1")
        
    def task_2_fail():
        execution_history.append("t2")
        raise ValueError("Simulated Validation Error")
        
    def task_3_downstream():
        execution_history.append("t3")

    orch.add_task("generate", task_1, max_retries=0)
    orch.add_task("validate", task_2_fail, dependencies=["generate"], max_retries=0)
    orch.add_task("transform", task_3_downstream, dependencies=["validate"], max_retries=0)
    
    summary = orch.run_dag()
    assert summary["passed"] is False
    assert orch.tasks["generate"].state == TaskState.SUCCESS
    assert orch.tasks["validate"].state == TaskState.FAILED
    assert orch.tasks["transform"].state == TaskState.UPSTREAM_FAILED
    
    # Verify task 3 was never executed
    assert "t3" not in execution_history
    assert execution_history == ["t1", "t2"]

def test_orchestrator_retry_behavior(tmp_path):
    """Test 4: Verify task retry behavior and successful recovery on second attempt."""
    state_file = os.path.join(tmp_path, "state.json")
    orch = Orchestrator(state_file_path=state_file)
    
    attempt_counter = [0]
    
    def flaky_task():
        attempt_counter[0] += 1
        if attempt_counter[0] == 1:
            raise RuntimeError("Transient Error on Attempt 1")

    orch.add_task("flaky", flaky_task, max_retries=2, retry_delay=0.1)
    summary = orch.run_dag()
    
    assert summary["passed"] is True
    assert orch.tasks["flaky"].state == TaskState.SUCCESS
    assert orch.tasks["flaky"].attempts == 2

def test_orchestrator_final_failure_after_max_retries(tmp_path):
    """Test 5: Verify task fails permanently after exhausting max_retries."""
    state_file = os.path.join(tmp_path, "state.json")
    orch = Orchestrator(state_file_path=state_file)
    
    def always_fails():
        raise RuntimeError("Permanent Failure")

    orch.add_task("broken", always_fails, max_retries=2, retry_delay=0.1)
    summary = orch.run_dag()
    
    assert summary["passed"] is False
    assert orch.tasks["broken"].state == TaskState.FAILED
    assert orch.tasks["broken"].attempts == 3  # Initial attempt + 2 retries

def test_orchestrator_state_file_persistence(orch_env):
    """Test 7: Verify orchestrator_state.json persistence structure."""
    raw, proc, db_file, state_file = orch_env
    orch = build_ecommerce_dag(raw_dir=raw, processed_dir=proc, db_path=db_file)
    orch.run_dag()
    
    assert os.path.exists(state_file)
    with open(state_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    assert "total_duration_seconds" in data
    assert "tasks" in data
    assert len(data["tasks"]) == 7
    assert data["tasks"]["generate_raw_data"]["state"] == "SUCCESS"

def test_orchestrator_idempotent_rerun(orch_env):
    """Test 8: Verify running the workflow multiple times is completely safe and reproducible."""
    raw, proc, db_file, _ = orch_env
    orch1 = build_ecommerce_dag(raw_dir=raw, processed_dir=proc, db_path=db_file)
    sum1 = orch1.run_dag()
    
    orch2 = build_ecommerce_dag(raw_dir=raw, processed_dir=proc, db_path=db_file)
    sum2 = orch2.run_dag()
    
    assert sum1["passed"] is True
    assert sum2["passed"] is True
    assert sum1["state_counts"] == sum2["state_counts"]
