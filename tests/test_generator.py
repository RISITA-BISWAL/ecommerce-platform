import os
import pytest
from src.generator import EcommerceDataGenerator

@pytest.fixture
def generator():
    return EcommerceDataGenerator(seed=42)

def test_generate_customers(generator):
    customers = generator.generate_customers(count=100)
    assert len(customers) == 100
    first = customers[0]
    assert "customer_id" in first
    assert "name" in first
    assert "email" in first
    assert "created_at" in first
    assert "signup_channel" in first
    assert first["customer_id"].startswith("CUST-")

def test_generate_products(generator):
    products = generator.generate_products(count=30)
    assert len(products) == 30
    first = products[0]
    assert "product_id" in first
    assert "name" in first
    assert "category" in first
    assert "price" in first
    assert "stock" in first
    assert first["price"] > 0

def test_generate_orders(generator):
    customers = generator.generate_customers(count=100)
    products = generator.generate_products(count=30)
    orders = generator.generate_orders(count=1000, customers=customers, products=products)
    
    assert len(orders) == 1000
    first = orders[0]
    assert "order_id" in first
    assert "customer_id" in first
    assert "product_id" in first
    assert "quantity" in first
    assert "total_price" in first
    assert "order_date" in first
    assert "status" in first

def test_generate_payments(generator):
    customers = generator.generate_customers(count=10)
    products = generator.generate_products(count=10)
    orders = generator.generate_orders(count=20, customers=customers, products=products)
    payments = generator.generate_payments(orders=orders)
    
    assert len(payments) == 20
    first = payments[0]
    assert "payment_id" in first
    assert "order_id" in first
    assert "payment_method" in first
    assert "amount" in first
    assert "status" in first
    assert "payment_date" in first

def test_generate_reviews(generator):
    customers = generator.generate_customers(count=50)
    products = generator.generate_products(count=20)
    reviews = generator.generate_reviews(count=300, customers=customers, products=products)
    
    assert len(reviews) == 300
    first = reviews[0]
    assert "review_id" in first
    assert "customer_id" in first
    assert "product_id" in first
    assert "rating" in first
    assert "review_text" in first
    assert "review_date" in first
    assert 1 <= first["rating"] <= 5

def test_referential_integrity(generator):
    customers = generator.generate_customers(count=100)
    products = generator.generate_products(count=30)
    orders = generator.generate_orders(count=1000, customers=customers, products=products)
    payments = generator.generate_payments(orders=orders)
    reviews = generator.generate_reviews(count=300, customers=customers, products=products)
    
    cust_ids = {c["customer_id"] for c in customers}
    prod_ids = {p["product_id"] for p in products}
    ord_ids = {o["order_id"] for o in orders}
    
    # Check all FK relationships
    for o in orders:
        assert o["customer_id"] in cust_ids
        assert o["product_id"] in prod_ids
        
    for p in payments:
        assert p["order_id"] in ord_ids
        
    for r in reviews:
        assert r["customer_id"] in cust_ids
        assert r["product_id"] in prod_ids

def test_validation_engine(generator):
    customers = generator.generate_customers(count=100)
    products = generator.generate_products(count=30)
    orders = generator.generate_orders(count=1000, customers=customers, products=products)
    payments = generator.generate_payments(orders=orders)
    reviews = generator.generate_reviews(count=300, customers=customers, products=products)
    
    # Valid datasets should pass cleanly
    report = generator.validate_datasets(customers, products, orders, payments, reviews)
    assert report["checks_passed"] is True
    
    # Corrupt a foreign key and test validation failure
    bad_orders = [dict(o) for o in orders]
    bad_orders[0]["customer_id"] = "CUST-NONEXISTENT"
    with pytest.raises(ValueError) as exc_info:
        generator.validate_datasets(customers, products, bad_orders, payments, reviews)
    assert "Broken FK" in str(exc_info.value)
