"""
Standalone Execution Script for Milestone 9 Incremental CDC & Observability.

Demonstrates:
1. Reading existing dataset state from data/raw/orders.csv.
2. Generating a simulated incremental delta batch (new orders + status updates).
3. Classifying delta records into INSERT, UPDATE, and NO_CHANGE using IncrementalCDCProcessor.
4. Performing idempotent SQLite UPSERT merge into data/ecommerce.db.
5. Tracking dataset lineage and task telemetry using PlatformObservabilityEngine.
6. Exporting lineage graph and telemetry JSON reports to data/processed/.
"""

import os
import sys
import time
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.incremental import IncrementalCDCProcessor
from src.observability import PlatformObservabilityEngine
from src.pipeline import EcommerceETLPipeline


def main():
    print("==================================================================")
    print("      MILESTONE 9 - INCREMENTAL CDC & OBSERVABILITY ENGINE      ")
    print("==================================================================\n")

    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    db_path = project_root / "data" / "ecommerce.db"

    # Ensure baseline database exists
    if not db_path.exists():
        print("[SETUP] Baseline SQLite database not found. Running ETL Pipeline first...")
        pipeline = EcommerceETLPipeline(
            raw_data_dir=str(raw_dir),
            processed_data_dir=str(processed_dir),
            db_path=str(db_path),
        )
        pipeline.run()

    # Initialize CDC Processor & Observability Engine
    cdc_processor = IncrementalCDCProcessor(primary_key="order_id")
    obs_engine = PlatformObservabilityEngine(threshold=0.20)

    # 1. Define Lineage Nodes & Edges
    obs_engine.add_lineage_node("raw_orders", "raw_csv", {"file": "data/raw/orders.csv"})
    obs_engine.add_lineage_node("delta_orders", "incremental_batch", {"type": "simulated_delta"})
    obs_engine.add_lineage_node(
        "sqlite_orders", "sqlite_table", {"db": "data/ecommerce.db", "table": "orders"}
    )
    obs_engine.add_lineage_node(
        "spark_parquet",
        "parquet_export",
        {"dir": "data/spark_processed/product_sales_summary"},
    )

    obs_engine.add_lineage_edge("raw_orders", "sqlite_orders", "INITIAL_LOAD")
    obs_engine.add_lineage_edge("delta_orders", "sqlite_orders", "CDC_MERGE")
    obs_engine.add_lineage_edge("sqlite_orders", "spark_parquet", "PARQUET_EXPORT")

    # 2. Simulate Incremental Batch (1 INSERT, 1 UPDATE, 1 NO_CHANGE)
    print("--- [1. SIMULATING INCREMENTAL DELTA BATCH] ---")
    simulated_delta = [
        # INSERT (New Order)
        {
            "order_id": "ORD9999",
            "customer_id": "CUST001",
            "product_id": "PROD001",
            "quantity": 2,
            "total_price": 99.99,
            "order_date": "2026-08-17 09:00:00",
            "status": "Completed",
        },
        # UPDATE (Status change for existing ORD00001)
        {
            "order_id": "ORD00001",
            "customer_id": "CUST00001",
            "product_id": "PROD00001",
            "quantity": 1,
            "total_price": 49.99,
            "order_date": "2026-01-01 10:00:00",
            "status": "Shipped",
        },
        # NO_CHANGE (Identical replay of existing ORD00002)
        {
            "order_id": "ORD00002",
            "customer_id": "CUST00002",
            "product_id": "PROD00002",
            "quantity": 2,
            "total_price": 120.00,
            "order_date": "2026-01-01 11:00:00",
            "status": "Pending",
        },
    ]

    print(f"Incoming Delta Records: {len(simulated_delta)}")

    # 3. Execute CDC Classification & Idempotent Upsert
    start_time = time.time()
    summary = cdc_processor.upsert_sqlite_table(
        db_path=str(db_path),
        table_name="orders",
        incoming_delta=simulated_delta,
    )
    elapsed = time.time() - start_time

    print("\n--- [2. CDC MERGE CLASSIFICATION SUMMARY] ---")
    print(f"  - INSERTS   : {summary['INSERT']}")
    print(f"  - UPDATES   : {summary['UPDATE']}")
    print(f"  - NO_CHANGE : {summary['NO_CHANGE']}")
    print(f"  - Total     : {summary['total_delta_records']}")
    print(f"  - Duration  : {elapsed:.4f}s")

    # 4. Record Telemetry Metrics
    obs_engine.record_execution_telemetry(
        task_name="cdc_incremental_upsert",
        duration_seconds=elapsed,
        row_count=summary["total_delta_records"],
        status="SUCCESS",
        details=summary,
    )

    # 5. Check Schema & Data Drift
    print("\n--- [3. DRIFT DETECTION GUARDRAILS] ---")
    expected_orders_schema = {
        "order_id": "TEXT",
        "customer_id": "TEXT",
        "product_id": "TEXT",
        "quantity": "INTEGER",
        "total_price": "REAL",
        "order_date": "TIMESTAMP",
        "status": "TEXT",
    }
    actual_orders_schema = dict(expected_orders_schema)
    schema_drift = obs_engine.detect_schema_drift(expected_orders_schema, actual_orders_schema)
    print(f"  Schema Drift Detected: {schema_drift['has_drift']}")

    data_drift = obs_engine.detect_data_drift(baseline_metric=1000.0, current_metric=1001.0)
    print(
        f"  Row Count Drift Detected: {data_drift['has_drift']} (Change: {data_drift['relative_change']*100:.2f}%)"
    )

    # 6. Export Observability & Lineage Reports
    lineage_file = str(processed_dir / "platform_lineage.json")
    telemetry_file = str(processed_dir / "execution_telemetry.json")

    obs_engine.export_lineage_json(lineage_file)
    obs_engine.export_telemetry_json(telemetry_file)

    print("\n------------------------------------------------------------------")
    print("OBSERVABILITY ARTIFACTS GENERATED:")
    print(f"  - Lineage Graph : '{lineage_file}'")
    print(f"  - Telemetry Log : '{telemetry_file}'")
    print("==================================================================")


if __name__ == "__main__":
    main()
