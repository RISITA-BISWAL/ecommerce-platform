import os
import sqlite3
import pytest
from src.generator import EcommerceDataGenerator
from src.database import EcommerceDatabase

@pytest.fixture
def temp_db(tmp_path):
    """Provides a fresh temporary SQLite database path."""
    db_file = os.path.join(tmp_path, "test_ecommerce.db")
    return EcommerceDatabase(db_path=db_file)

@pytest.fixture
def raw_data_dir(tmp_path):
    """Generates synthetic CSV files in a temporary directory."""
    raw_dir = os.path.join(tmp_path, "raw")
    gen = EcommerceDataGenerator(seed=123)
    customers = gen.generate_customers(100)
    products = gen.generate_products(30)
    orders = gen.generate_orders(1000, customers, products)
    payments = gen.generate_payments(orders)
    reviews = gen.generate_reviews(300, customers, products)
    
    gen.save_to_csv(customers, os.path.join(raw_dir, "customers.csv"))
    gen.save_to_csv(products, os.path.join(raw_dir, "products.csv"))
    gen.save_to_csv(orders, os.path.join(raw_dir, "orders.csv"))
    gen.save_to_csv(payments, os.path.join(raw_dir, "payments.csv"))
    gen.save_to_csv(reviews, os.path.join(raw_dir, "reviews.csv"))
    return raw_dir

def test_database_table_creation(temp_db):
    """Test that all 5 relational tables are created."""
    temp_db.create_tables()
    conn = temp_db.get_connection()
    cursor = conn.cursor()
    
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    table_names = {t[0] for t in tables}
    conn.close()
    
    expected_tables = {"customers", "products", "orders", "payments", "reviews"}
    assert expected_tables.issubset(table_names)

def test_foreign_key_enforcement(temp_db):
    """Test that inserting a child record with a non-existent parent FK raises IntegrityError."""
    temp_db.create_tables()
    conn = temp_db.get_connection()
    cursor = conn.cursor()
    
    # Inserting an order with invalid customer_id and product_id should fail
    with pytest.raises(sqlite3.IntegrityError):
        cursor.execute("""
            INSERT INTO orders (order_id, customer_id, product_id, quantity, total_price, order_date, status)
            VALUES ('ORD-99999', 'CUST-NONEXISTENT', 'PROD-NONEXISTENT', 1, 10.0, '2026-01-01 10:00:00', 'Completed')
        """)
    conn.close()

def test_csv_ingestion_and_counts(temp_db, raw_data_dir):
    """Test CSV ingestion into SQLite and verify exact record counts."""
    temp_db.load_csv_data(raw_data_dir=raw_data_dir)
    counts = temp_db.get_row_counts()
    
    assert counts["customers"] == 100
    assert counts["products"] == 30
    assert counts["orders"] == 1000
    assert counts["payments"] == 1000
    assert counts["reviews"] == 300

def test_idempotent_loading(temp_db, raw_data_dir):
    """Test that loading CSV data multiple times does not produce duplicate records."""
    temp_db.load_csv_data(raw_data_dir=raw_data_dir)
    # Second ingestion run
    temp_db.load_csv_data(raw_data_dir=raw_data_dir)
    
    counts = temp_db.get_row_counts()
    assert counts["customers"] == 100
    assert counts["products"] == 30
    assert counts["orders"] == 1000
    assert counts["payments"] == 1000
    assert counts["reviews"] == 300

def test_foreign_key_integrity_check(temp_db, raw_data_dir):
    """Test PRAGMA foreign_key_check returns 0 violations after loading data."""
    temp_db.load_csv_data(raw_data_dir=raw_data_dir)
    violations = temp_db.verify_foreign_keys()
    assert len(violations) == 0
