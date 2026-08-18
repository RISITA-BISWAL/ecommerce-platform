import json
import os
import sqlite3
import pandas as pd
import pytest
from src.generator import EcommerceDataGenerator
from src.pipeline import EcommerceETLPipeline, ETLValidationError
from src.database import EcommerceDatabase

@pytest.fixture
def pipeline_env(tmp_path):
    """Sets up temporary raw, processed, and database paths for pipeline testing."""
    raw = os.path.join(tmp_path, "raw")
    proc = os.path.join(tmp_path, "processed")
    db_file = os.path.join(tmp_path, "ecommerce.db")
    
    gen = EcommerceDataGenerator(seed=42)
    cust = gen.generate_customers(100)
    prod = gen.generate_products(30)
    ord_ = gen.generate_orders(1000, cust, prod)
    pay = gen.generate_payments(ord_)
    rev = gen.generate_reviews(300, cust, prod)
    
    gen.save_to_csv(cust, os.path.join(raw, "customers.csv"))
    gen.save_to_csv(prod, os.path.join(raw, "products.csv"))
    gen.save_to_csv(ord_, os.path.join(raw, "orders.csv"))
    gen.save_to_csv(pay, os.path.join(raw, "payments.csv"))
    gen.save_to_csv(rev, os.path.join(raw, "reviews.csv"))
    
    return raw, proc, db_file

def test_pipeline_success_path(pipeline_env):
    """Test full pipeline success execution and verification."""
    raw, proc, db_file = pipeline_env
    pipeline = EcommerceETLPipeline(raw_data_dir=raw, processed_data_dir=proc, db_path=db_file)
    
    result = pipeline.run()
    assert result["status"] == "SUCCESS"
    assert result["row_counts"]["customers"] == 100
    assert result["row_counts"]["products"] == 30
    assert result["row_counts"]["orders"] == 1000
    assert result["row_counts"]["payments"] == 1000
    assert result["row_counts"]["reviews"] == 300
    assert result["fk_violations"] == 0
    
    # Check that quality report exists
    report_path = os.path.join(proc, "data_quality_report.json")
    assert os.path.exists(report_path)
    with open(report_path, "r", encoding="utf-8") as f:
        rep = json.load(f)
    assert rep["summary"]["overall_passed"] is True

def test_pipeline_halts_on_validation_failure(tmp_path):
    """
    Failure Path Test:
    Intentionally introduces corrupt raw data and verifies that:
    - Validation fails.
    - ETLValidationError is raised.
    - Quality report IS saved to disk.
    - Transformation and database loading do NOT proceed.
    """
    raw = os.path.join(tmp_path, "raw_corrupt")
    proc = os.path.join(tmp_path, "processed")
    db_file = os.path.join(tmp_path, "ecommerce.db")
    os.makedirs(raw, exist_ok=True)
    
    # Create corrupt raw datasets (Invalid rating = 99)
    cust_df = pd.DataFrame([{"customer_id": "CUST-0001", "name": "Alice", "email": "alice@test.com", "created_at": "2026-01-01 10:00:00", "signup_channel": "Web"}])
    prod_df = pd.DataFrame([{"product_id": "PROD-0001", "name": "Item 1", "category": "Books", "price": 10.0, "stock": 5}])
    ord_df = pd.DataFrame([{"order_id": "ORD-0001", "customer_id": "CUST-0001", "product_id": "PROD-0001", "quantity": 1, "total_price": 10.0, "order_date": "2026-01-01 11:00:00", "status": "Completed"}])
    pay_df = pd.DataFrame([{"payment_id": "PAY-0001", "order_id": "ORD-0001", "payment_method": "Card", "amount": 10.0, "status": "Completed", "payment_date": "2026-01-01 11:05:00"}])
    rev_df = pd.DataFrame([{"review_id": "REV-0001", "customer_id": "CUST-0001", "product_id": "PROD-0001", "rating": 99, "review_text": "Corrupt", "review_date": "2026-01-01 12:00:00"}])
    
    cust_df.to_csv(os.path.join(raw, "customers.csv"), index=False)
    prod_df.to_csv(os.path.join(raw, "products.csv"), index=False)
    ord_df.to_csv(os.path.join(raw, "orders.csv"), index=False)
    pay_df.to_csv(os.path.join(raw, "payments.csv"), index=False)
    rev_df.to_csv(os.path.join(raw, "reviews.csv"), index=False)
    
    pipeline = EcommerceETLPipeline(raw_data_dir=raw, processed_data_dir=proc, db_path=db_file)
    
    # Assert ETLValidationError is raised
    with pytest.raises(ETLValidationError) as exc_info:
        pipeline.run()
    assert "Data quality audit failed" in str(exc_info.value)
    
    # Assert Quality report WAS written to disk despite failure
    report_path = os.path.join(proc, "data_quality_report.json")
    assert os.path.exists(report_path)
    with open(report_path, "r", encoding="utf-8") as f:
        rep = json.load(f)
    assert rep["summary"]["overall_passed"] is False
    assert rep["datasets"]["reviews"]["invalid_value_counts"]["invalid_ratings"] == 1
    
    # Assert Processed CSV files were NOT generated
    assert not os.path.exists(os.path.join(proc, "customers.csv"))
    
    # Assert Database file was NOT written with data
    if os.path.exists(db_file):
        db = EcommerceDatabase(db_path=db_file)
        counts = db.get_row_counts()
        assert sum(counts.values()) == 0

def test_pipeline_idempotency(pipeline_env):
    """
    Idempotency Test:
    Runs the pipeline multiple times and verifies:
    - Same row counts.
    - Zero duplicate primary IDs.
    - Zero foreign-key violations.
    """
    raw, proc, db_file = pipeline_env
    pipeline = EcommerceETLPipeline(raw_data_dir=raw, processed_data_dir=proc, db_path=db_file)
    
    # Run 1
    res1 = pipeline.run()
    # Run 2 (Second execution)
    res2 = pipeline.run()
    
    # Verify row counts are identical across runs
    assert res1["row_counts"] == res2["row_counts"]
    assert res2["row_counts"]["customers"] == 100
    assert res2["row_counts"]["orders"] == 1000
    assert res2["fk_violations"] == 0
    
    # Check no duplicate primary IDs exist in SQLite tables
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    for table, pk in [("customers", "customer_id"), ("products", "product_id"), ("orders", "order_id"), ("payments", "payment_id"), ("reviews", "review_id")]:
        cursor.execute(f"SELECT COUNT({pk}), COUNT(DISTINCT {pk}) FROM {table}")
        tot, dist = cursor.fetchone()
        assert tot == dist
    conn.close()

def test_raw_data_immutability(pipeline_env):
    """Verifies that raw CSV files in data/raw/ remain 100% untouched."""
    raw, proc, db_file = pipeline_env
    
    raw_cust_before = pd.read_csv(os.path.join(raw, "customers.csv"))
    raw_orders_before = pd.read_csv(os.path.join(raw, "orders.csv"))
    
    pipeline = EcommerceETLPipeline(raw_data_dir=raw, processed_data_dir=proc, db_path=db_file)
    pipeline.run()
    
    raw_cust_after = pd.read_csv(os.path.join(raw, "customers.csv"))
    raw_orders_after = pd.read_csv(os.path.join(raw, "orders.csv"))
    
    assert raw_cust_before.equals(raw_cust_after)
    assert raw_orders_before.equals(raw_orders_after)
