import os
import sqlite3
import pandas as pd
import pytest
from src.generator import EcommerceDataGenerator
from src.database import EcommerceDatabase
from src.pipeline import EcommerceETLPipeline
from src.analytics import EcommerceAnalytics

@pytest.fixture
def analytics_db(tmp_path):
    """Sets up a fully populated test database."""
    raw = os.path.join(tmp_path, "raw")
    proc = os.path.join(tmp_path, "processed")
    db_file = os.path.join(tmp_path, "ecommerce.db")
    
    # Generate datasets
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
    
    # Run pipeline to populate SQLite database
    pipeline = EcommerceETLPipeline(raw_data_dir=raw, processed_data_dir=proc, db_path=db_file)
    pipeline.run()
    
    return db_file

@pytest.fixture
def empty_db(tmp_path):
    """Sets up an empty SQLite database with tables but 0 records."""
    db_file = os.path.join(tmp_path, "empty_ecommerce.db")
    db = EcommerceDatabase(db_path=db_file)
    db.create_tables()
    return db_file

def test_executive_kpis(analytics_db):
    """Test executive KPI summary calculation."""
    analytics = EcommerceAnalytics(db_path=analytics_db)
    kpis = analytics.get_executive_kpis()
    
    assert isinstance(kpis, pd.DataFrame)
    assert len(kpis) == 1
    assert kpis["total_customers"].iloc[0] == 100
    assert kpis["total_products"].iloc[0] == 30
    assert kpis["total_orders"].iloc[0] == 1000
    assert kpis["total_revenue"].iloc[0] > 0
    assert kpis["average_order_value"].iloc[0] > 0
    assert kpis["successful_payments"].iloc[0] > 0
    assert 1.0 <= kpis["average_product_rating"].iloc[0] <= 5.0

def test_revenue_reconciliation(analytics_db):
    """Test revenue reconciliation returns 0 discrepancies on clean data."""
    analytics = EcommerceAnalytics(db_path=analytics_db)
    reconciled = analytics.reconcile_revenue()
    
    assert isinstance(reconciled, pd.DataFrame)
    assert len(reconciled) == 0  # 0 discrepancies expected

def test_sales_analytics(analytics_db):
    """Test sales summary and order status breakdown."""
    analytics = EcommerceAnalytics(db_path=analytics_db)
    sales = analytics.get_overall_sales_summary()
    assert sales["total_orders"].iloc[0] == 1000
    
    status_df = analytics.get_orders_by_status()
    assert len(status_df) > 0
    assert "order_status" in status_df.columns
    assert "total_revenue" in status_df.columns

def test_product_analytics(analytics_db):
    """Test top selling products and category revenue."""
    analytics = EcommerceAnalytics(db_path=analytics_db)
    top_prod = analytics.get_top_selling_products(limit=5)
    assert len(top_prod) == 5
    assert top_prod["total_revenue"].iloc[0] >= top_prod["total_revenue"].iloc[1]
    
    cat_df = analytics.get_revenue_by_category()
    assert len(cat_df) > 0
    assert "category" in cat_df.columns

def test_customer_analytics_left_join(tmp_path):
    """Test customer analytics includes customers with 0 orders via LEFT JOIN."""
    db_file = os.path.join(tmp_path, "cust_test.db")
    db = EcommerceDatabase(db_path=db_file)
    db.create_tables()
    
    # Insert 1 customer with orders and 1 customer with 0 orders
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO customers VALUES ('CUST-001', 'Active', 'a@test.com', '2026-01-01', 'Web');")
    cursor.execute("INSERT INTO customers VALUES ('CUST-002', 'Inactive', 'i@test.com', '2026-01-01', 'Web');")
    cursor.execute("INSERT INTO products VALUES ('PROD-001', 'Item', 'Cat', 10.0, 10);")
    cursor.execute("INSERT INTO orders VALUES ('ORD-001', 'CUST-001', 'PROD-001', 2, 20.0, '2026-01-02', 'Completed');")
    conn.commit()
    conn.close()
    
    analytics = EcommerceAnalytics(db_path=db_file)
    cust_df = analytics.get_customer_analytics()
    
    assert len(cust_df) == 2
    cust_002 = cust_df[cust_df["customer_id"] == "CUST-002"].iloc[0]
    assert cust_002["total_orders"] == 0
    assert cust_002["total_spend"] == 0.0

def test_payment_and_review_analytics(analytics_db):
    """Test payment distribution and review rating sentiment."""
    analytics = EcommerceAnalytics(db_path=analytics_db)
    pay_dist = analytics.get_payment_method_distribution()
    assert len(pay_dist) > 0
    
    ratings_df = analytics.get_product_review_ratings(limit=10)
    assert len(ratings_df) <= 10
    
    sentiment_df = analytics.get_rating_category_distribution()
    assert len(sentiment_df) > 0
    assert "sentiment_category" in sentiment_df.columns

def test_monthly_trends(analytics_db):
    """Test monthly trends query."""
    analytics = EcommerceAnalytics(db_path=analytics_db)
    monthly = analytics.get_monthly_trends()
    assert len(monthly) > 0
    assert "order_month" in monthly.columns

def test_empty_database_safety(empty_db):
    """Test running all analytics queries on an empty database returns empty DataFrames without errors."""
    analytics = EcommerceAnalytics(db_path=empty_db)
    
    assert len(analytics.get_executive_kpis()) == 1
    assert len(analytics.get_reconcile_revenue() if hasattr(analytics, "get_reconcile_revenue") else analytics.reconcile_revenue()) == 0
    assert len(analytics.get_overall_sales_summary()) == 1
    assert len(analytics.get_orders_by_status()) == 0
    assert len(analytics.get_top_selling_products()) == 0
    assert len(analytics.get_revenue_by_category()) == 0
    assert len(analytics.get_customer_analytics()) == 0
    assert len(analytics.get_payment_method_distribution()) == 0
    assert len(analytics.get_product_review_ratings()) == 0
    assert len(analytics.get_monthly_trends()) == 0

def test_read_only_database_behavior(analytics_db):
    """Verifies that running analytics does not modify table records or database state."""
    db = EcommerceDatabase(db_path=analytics_db)
    counts_before = db.get_row_counts()
    
    analytics = EcommerceAnalytics(db_path=analytics_db)
    analytics.get_executive_kpis()
    analytics.get_top_selling_products()
    analytics.get_customer_analytics()
    analytics.reconcile_revenue()
    
    counts_after = db.get_row_counts()
    assert counts_before == counts_after
