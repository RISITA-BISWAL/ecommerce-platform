import json
import os
import pandas as pd
import pytest
from src.generator import EcommerceDataGenerator
from src.validator import DataQualityAuditor

@pytest.fixture
def raw_dir(tmp_path):
    """Generates clean raw datasets in a temporary directory."""
    raw = os.path.join(tmp_path, "raw")
    gen = EcommerceDataGenerator(seed=100)
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
    return raw

def test_validator_clean_data(raw_dir, tmp_path):
    """Test that valid raw datasets pass the data quality audit cleanly."""
    report_file = os.path.join(tmp_path, "processed", "data_quality_report.json")
    auditor = DataQualityAuditor(raw_data_dir=raw_dir)
    report = auditor.audit_datasets(report_output_path=report_file)
    
    assert report["summary"]["overall_passed"] is True
    assert report["summary"]["total_issues_found"] == 0
    assert os.path.exists(report_file)
    
    with open(report_file, "r", encoding="utf-8") as f:
        saved_report = json.load(f)
    assert saved_report["summary"]["overall_passed"] is True

def test_validator_detects_corrupt_data(tmp_path):
    """Test that intentionally corrupted datasets are flagged across rules."""
    raw = os.path.join(tmp_path, "raw_corrupt")
    os.makedirs(raw, exist_ok=True)
    
    # 1. Corrupt Customers: Duplicate ID & Missing email
    cust_df = pd.DataFrame([
        {"customer_id": "CUST-0001", "name": "Alice", "email": "alice@test.com", "created_at": "2026-01-01 10:00:00", "signup_channel": "Web"},
        {"customer_id": "CUST-0001", "name": "Bob", "email": None, "created_at": "2026-01-02 10:00:00", "signup_channel": "Web"}
    ])
    
    # 2. Corrupt Products: Negative price & Invalid stock
    prod_df = pd.DataFrame([
        {"product_id": "PROD-0001", "name": "Item 1", "category": "Books", "price": -10.0, "stock": -5}
    ])
    
    # 3. Corrupt Orders: Invalid Qty, Bad Date, Bad Status, Broken FK
    ord_df = pd.DataFrame([
        {"order_id": "ORD-0001", "customer_id": "CUST-BAD", "product_id": "PROD-0001", "quantity": -2, "total_price": 0.0, "order_date": "INVALID_DATE", "status": "UNKNOWN_STATUS"}
    ])
    
    # 4. Corrupt Payments: Negative amount, Bad Status, Broken FK
    pay_df = pd.DataFrame([
        {"payment_id": "PAY-0001", "order_id": "ORD-BAD", "payment_method": "Card", "amount": -50.0, "status": "INVALID_PAY_STATUS", "payment_date": "2026-01-01 10:05:00"}
    ])
    
    # 5. Corrupt Reviews: Rating out of bounds (6), Broken FK
    rev_df = pd.DataFrame([
        {"review_id": "REV-0001", "customer_id": "CUST-BAD", "product_id": "PROD-BAD", "rating": 6, "review_text": "Bad", "review_date": "2026-01-01 12:00:00"}
    ])
    
    cust_df.to_csv(os.path.join(raw, "customers.csv"), index=False)
    prod_df.to_csv(os.path.join(raw, "products.csv"), index=False)
    ord_df.to_csv(os.path.join(raw, "orders.csv"), index=False)
    pay_df.to_csv(os.path.join(raw, "payments.csv"), index=False)
    rev_df.to_csv(os.path.join(raw, "reviews.csv"), index=False)
    
    report_file = os.path.join(tmp_path, "processed", "data_quality_report.json")
    auditor = DataQualityAuditor(raw_data_dir=raw)
    report = auditor.audit_datasets(report_output_path=report_file)
    
    assert report["summary"]["overall_passed"] is False
    assert report["summary"]["total_issues_found"] > 0
    
    # Check specific fields in dataset report
    cust_rep = report["datasets"]["customers"]
    assert cust_rep["duplicate_id_count"] == 1
    assert cust_rep["missing_value_count"] == 1
    
    prod_rep = report["datasets"]["products"]
    assert prod_rep["invalid_value_counts"]["invalid_prices"] == 1
    
    ord_rep = report["datasets"]["orders"]
    assert ord_rep["foreign_key_violations"] >= 1
    assert ord_rep["date_errors"] == 1
    assert ord_rep["status_errors"] == 1
    
    rev_rep = report["datasets"]["reviews"]
    assert rev_rep["invalid_value_counts"]["invalid_ratings"] == 1
