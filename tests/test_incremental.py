"""
Unit tests for Milestone 9 Incremental CDC Processor (IncrementalCDCProcessor).
"""

import os
import sqlite3
import sys
from pathlib import Path
import pytest

# Ensure project root is on sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.incremental import IncrementalCDCProcessor


@pytest.fixture
def sample_current_orders():
    return [
        {"order_id": "ORD001", "customer_id": "CUST01", "total_price": 100.0, "status": "Completed"},
        {"order_id": "ORD002", "customer_id": "CUST02", "total_price": 250.0, "status": "Pending"},
    ]


def test_insert_classification(sample_current_orders):
    """Test classifying new records as INSERT."""
    processor = IncrementalCDCProcessor(primary_key="order_id")
    delta = [
        {"order_id": "ORD003", "customer_id": "CUST03", "total_price": 300.0, "status": "Completed"}
    ]
    res = processor.classify_delta(sample_current_orders, delta)
    assert res["summary"]["INSERT"] == 1
    assert res["summary"]["UPDATE"] == 0
    assert res["summary"]["NO_CHANGE"] == 0
    assert res["inserts"][0]["_cdc_action"] == "INSERT"


def test_update_classification(sample_current_orders):
    """Test classifying modified existing records as UPDATE and tracking changed columns."""
    processor = IncrementalCDCProcessor(primary_key="order_id")
    delta = [
        {"order_id": "ORD002", "customer_id": "CUST02", "total_price": 250.0, "status": "Completed"}
    ]
    res = processor.classify_delta(sample_current_orders, delta)
    assert res["summary"]["INSERT"] == 0
    assert res["summary"]["UPDATE"] == 1
    assert res["summary"]["NO_CHANGE"] == 0
    assert res["updates"][0]["_cdc_action"] == "UPDATE"
    assert "status" in res["updates"][0]["_changed_columns"]


def test_no_change_classification(sample_current_orders):
    """Test classifying identical existing records as NO_CHANGE."""
    processor = IncrementalCDCProcessor(primary_key="order_id")
    delta = [
        {"order_id": "ORD001", "customer_id": "CUST01", "total_price": 100.0, "status": "Completed"}
    ]
    res = processor.classify_delta(sample_current_orders, delta)
    assert res["summary"]["INSERT"] == 0
    assert res["summary"]["UPDATE"] == 0
    assert res["summary"]["NO_CHANGE"] == 1
    assert res["no_changes"][0]["_cdc_action"] == "NO_CHANGE"


def test_idempotent_merge(sample_current_orders):
    """Test merging incoming delta into dataset is idempotent."""
    processor = IncrementalCDCProcessor(primary_key="order_id")
    delta = [
        {"order_id": "ORD002", "customer_id": "CUST02", "total_price": 250.0, "status": "Completed"},
        {"order_id": "ORD003", "customer_id": "CUST03", "total_price": 300.0, "status": "Completed"},
    ]

    # First merge
    merged_1, summary_1 = processor.merge_into_dataset(sample_current_orders, delta)
    assert len(merged_1) == 3
    assert summary_1["INSERT"] == 1
    assert summary_1["UPDATE"] == 1

    # Second merge with identical delta (should be idempotent with 0 INSERT/UPDATE)
    merged_2, summary_2 = processor.merge_into_dataset(merged_1, delta)
    assert len(merged_2) == 3
    assert summary_2["INSERT"] == 0
    assert summary_2["UPDATE"] == 0
    assert summary_2["NO_CHANGE"] == 2


def test_correct_final_database_state(tmp_path, sample_current_orders):
    """Test SQLite UPSERT operation produces correct final database state."""
    db_path = str(tmp_path / "test_cdc.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            total_price REAL,
            status TEXT
        )
    """)
    for r in sample_current_orders:
        cursor.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?)",
            (r["order_id"], r["customer_id"], r["total_price"], r["status"]),
        )
    conn.commit()
    conn.close()

    processor = IncrementalCDCProcessor(primary_key="order_id")
    delta = [
        {"order_id": "ORD002", "customer_id": "CUST02", "total_price": 275.0, "status": "Completed"},
        {"order_id": "ORD003", "customer_id": "CUST03", "total_price": 150.0, "status": "Pending"},
    ]

    summary = processor.upsert_sqlite_table(db_path, "orders", delta)
    assert summary["INSERT"] == 1
    assert summary["UPDATE"] == 1

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM orders ORDER BY order_id")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    ord2 = [r for r in rows if r[0] == "ORD002"][0]
    assert ord2[2] == 275.0
    assert ord2[3] == "Completed"
