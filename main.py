import os
from src.generator import EcommerceDataGenerator

def main():
    print("[INFO] Starting Milestone 1 Synthetic E-Commerce Data Generation...")
    
    # Instantiate the data generator with fixed seed for reproducibility
    generator = EcommerceDataGenerator(seed=42)
    
    # Generate all 5 required datasets
    print(" [CUSTOMERS] Generating 100 Customers...")
    customers = generator.generate_customers(count=100)
    
    print(" [PRODUCTS] Generating 30 Products...")
    products = generator.generate_products(count=30)
    
    print(" [ORDERS] Generating 1,000 Orders...")
    orders = generator.generate_orders(count=1000, customers=customers, products=products)
    
    print(" [PAYMENTS] Generating 1,000 Payments...")
    payments = generator.generate_payments(orders=orders)
    
    print(" [REVIEWS] Generating 300 Reviews...")
    reviews = generator.generate_reviews(count=300, customers=customers, products=products)
    
    # Validate datasets for nulls, duplicates, numeric constraints, and foreign key integrity
    print(" [VALIDATION] Running automated data quality and referential integrity checks...")
    generator.validate_datasets(customers, products, orders, payments, reviews)
    print(" [VALIDATION] All dataset quality and referential integrity checks PASSED!")
    
    # Target directory for raw data
    raw_data_dir = os.path.join("data", "raw")
    
    # Save all 5 datasets in CSV format
    generator.save_to_csv(customers, os.path.join(raw_data_dir, "customers.csv"))
    generator.save_to_csv(products, os.path.join(raw_data_dir, "products.csv"))
    generator.save_to_csv(orders, os.path.join(raw_data_dir, "orders.csv"))
    generator.save_to_csv(payments, os.path.join(raw_data_dir, "payments.csv"))
    generator.save_to_csv(reviews, os.path.join(raw_data_dir, "reviews.csv"))
    
    print(" [SUCCESS] Raw CSV datasets successfully generated in 'data/raw/':")
    print(f"   - customers.csv : {len(customers)} records")
    print(f"   - products.csv  : {len(products)} records")
    print(f"   - orders.csv    : {len(orders)} records")
    print(f"   - payments.csv  : {len(payments)} records")
    print(f"   - reviews.csv   : {len(reviews)} records")

if __name__ == "__main__":
    main()
