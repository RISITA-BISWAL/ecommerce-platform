"""
Unit tests for Milestone 10 Data Warehouse & Star Schema Engine.
"""

import sqlite3
import pytest
from src.generator import EcommerceDataGenerator
from src.database import EcommerceDatabase
from src.warehouse import EcommerceDataWarehouse


@pytest.fixture
def temp_oltp_and_dw(tmp_path):
    """Fixture initializing temporary OLTP database and Data Warehouse instance."""
    oltp_db_path = str(tmp_path / "test_oltp.db")
    dw_db_path = str(tmp_path / "test_dw.db")

    # Generate synthetic OLTP data
    gen = EcommerceDataGenerator(seed=42)
    customers = gen.generate_customers(15)
    products = gen.generate_products(5)
    orders = gen.generate_orders(20, customers=customers, products=products)
    payments = gen.generate_payments(orders=orders)
    reviews = gen.generate_reviews(10, customers=customers, products=products)

    # Save to temporary CSVs
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gen.save_to_csv(customers, str(raw_dir / "customers.csv"))
    gen.save_to_csv(products, str(raw_dir / "products.csv"))
    gen.save_to_csv(orders, str(raw_dir / "orders.csv"))
    gen.save_to_csv(payments, str(raw_dir / "payments.csv"))
    gen.save_to_csv(reviews, str(raw_dir / "reviews.csv"))

    # Load into OLTP SQLite DB
    oltp_db = EcommerceDatabase(db_path=oltp_db_path)
    oltp_db.create_tables()
    oltp_db.load_csv_data(str(raw_dir))

    # Create Data Warehouse manager instance
    dw = EcommerceDataWarehouse(db_path=oltp_db_path, dw_path=dw_db_path)
    return dw, oltp_db_path, dw_db_path


def test_create_warehouse_schema(temp_oltp_and_dw):
    """Test creation of star schema dimension and fact tables."""
    dw, _, dw_db_path = temp_oltp_and_dw
    dw.create_warehouse_schema()

    conn = sqlite3.connect(dw_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()

    assert "dim_customer" in tables
    assert "dim_product" in tables
    assert "dim_date" in tables
    assert "fact_sales" in tables


def test_populate_dim_date(temp_oltp_and_dw):
    """Test populating continuous calendar date dimension."""
    dw, _, dw_db_path = temp_oltp_and_dw
    dw.create_warehouse_schema()
    count = dw.populate_dim_date(start_year=2025, end_year=2025)

    assert count == 365

    conn = sqlite3.connect(dw_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT full_date, is_weekend FROM dim_date WHERE date_key = 20250101")
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "2025-01-01"


def test_populate_dim_customer(temp_oltp_and_dw):
    """Test extracting OLTP customers and loading dim_customer."""
    dw, _, dw_db_path = temp_oltp_and_dw
    dw.create_warehouse_schema()
    count = dw.populate_dim_customer()

    assert count == 15

    conn = sqlite3.connect(dw_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dim_customer")
    total = cursor.fetchone()[0]
    conn.close()

    assert total == 15


def test_populate_dim_product(temp_oltp_and_dw):
    """Test extracting OLTP products and loading dim_product."""
    dw, _, dw_db_path = temp_oltp_and_dw
    dw.create_warehouse_schema()
    count = dw.populate_dim_product()

    assert count == 5

    conn = sqlite3.connect(dw_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dim_product")
    total = cursor.fetchone()[0]
    conn.close()

    assert total == 5


def test_populate_fact_sales(temp_oltp_and_dw):
    """Test resolving surrogate keys and populating fact_sales table."""
    dw, _, dw_db_path = temp_oltp_and_dw
    dw.create_warehouse_schema()
    dw.populate_dim_date(start_year=2024, end_year=2026)
    dw.populate_dim_customer()
    dw.populate_dim_product()
    facts_count = dw.populate_fact_sales()

    assert facts_count == 20

    conn = sqlite3.connect(dw_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM fact_sales")
    total_facts = cursor.fetchone()[0]
    conn.close()

    assert total_facts == 20


def test_warehouse_idempotency(temp_oltp_and_dw):
    """Test that running build_full_warehouse multiple times produces identical state without duplicates."""
    dw, _, dw_db_path = temp_oltp_and_dw
    res1 = dw.build_full_warehouse()
    res2 = dw.build_full_warehouse()

    assert res1["status"] == "SUCCESS"
    assert res2["status"] == "SUCCESS"

    conn = sqlite3.connect(dw_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM fact_sales")
    total_facts = cursor.fetchone()[0]
    conn.close()

    assert total_facts == 20


def test_olap_analytics_queries(temp_oltp_and_dw):
    """Test OLAP aggregation queries against star schema warehouse."""
    dw, _, _ = temp_oltp_and_dw
    dw.build_full_warehouse()

    df_drilldown = dw.get_category_revenue_drilldown()
    df_weekend = dw.get_weekend_sales_performance()

    assert not df_drilldown.empty
    assert "category_revenue" in df_drilldown.columns

    assert not df_weekend.empty
    assert "day_type" in df_weekend.columns
    assert "total_revenue" in df_weekend.columns
