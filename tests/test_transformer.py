import os
import pandas as pd
import pytest
from src.generator import EcommerceDataGenerator
from src.transformer import DataTransformer

@pytest.fixture
def raw_and_processed_dirs(tmp_path):
    """Sets up separate raw and processed test directories."""
    raw = os.path.join(tmp_path, "raw")
    proc = os.path.join(tmp_path, "processed")
    gen = EcommerceDataGenerator(seed=100)
    cust = gen.generate_customers(50)
    prod = gen.generate_products(20)
    ord_ = gen.generate_orders(200, cust, prod)
    pay = gen.generate_payments(ord_)
    rev = gen.generate_reviews(50, cust, prod)
    
    # Introduce an un-lowercased email and untrimmed text in raw data
    cust[0]["email"] = "  TEST.USER@EXAMPLE.COM  "
    cust[0]["name"] = "  Alice Smith  "
    
    gen.save_to_csv(cust, os.path.join(raw, "customers.csv"))
    gen.save_to_csv(prod, os.path.join(raw, "products.csv"))
    gen.save_to_csv(ord_, os.path.join(raw, "orders.csv"))
    gen.save_to_csv(pay, os.path.join(raw, "payments.csv"))
    gen.save_to_csv(rev, os.path.join(raw, "reviews.csv"))
    
    return raw, proc

def test_transformation_pipeline(raw_and_processed_dirs):
    """Test full data transformation pipeline execution."""
    raw_dir, proc_dir = raw_and_processed_dirs
    
    # Read raw email before transformation to verify immutability
    raw_cust_before = pd.read_csv(os.path.join(raw_dir, "customers.csv"))
    assert "  TEST.USER@EXAMPLE.COM  " in raw_cust_before["email"].values
    
    transformer = DataTransformer(raw_data_dir=raw_dir, processed_data_dir=proc_dir)
    processed_dfs = transformer.transform_datasets()
    
    # 1. Processed CSV files creation
    for name in ["customers", "products", "orders", "payments", "reviews"]:
        assert os.path.exists(os.path.join(proc_dir, f"{name}.csv"))

    # 2. Raw files remain 100% unchanged
    raw_cust_after = pd.read_csv(os.path.join(raw_dir, "customers.csv"))
    assert raw_cust_before.equals(raw_cust_after)

    # 3. Text Standardization
    proc_cust = processed_dfs["customers"]
    assert proc_cust.iloc[0]["email"] == "test.user@example.com"
    assert proc_cust.iloc[0]["name"] == "Alice Smith"

    # 4. Revenue Verification & Recalculation
    proc_orders = processed_dfs["orders"]
    assert "calculated_total_price" in proc_orders.columns
    assert "has_price_discrepancy" in proc_orders.columns
    assert (proc_orders["total_price"] == proc_orders["calculated_total_price"]).all()

    # 5. Analytical Feature Engineering
    # Orders features
    assert "order_year" in proc_orders.columns
    assert "order_month" in proc_orders.columns
    assert "order_dayofweek" in proc_orders.columns
    assert proc_orders["order_year"].iloc[0] >= 2024

    # Customer features
    assert "customer_tenure_days" in proc_cust.columns
    assert (proc_cust["customer_tenure_days"] >= 0).all()

    # Reviews features
    proc_reviews = processed_dfs["reviews"]
    assert "rating_category" in proc_reviews.columns
    assert set(proc_reviews["rating_category"].unique()).issubset({"Positive", "Neutral", "Negative"})
