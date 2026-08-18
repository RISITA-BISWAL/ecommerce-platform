import os
from src.generator import EcommerceDataGenerator
from src.pipeline import EcommerceETLPipeline

def main():
    print("[INFO] Initiating E-Commerce ETL Pipeline Entrypoint...")
    
    raw_dir = os.path.join("data", "raw")
    processed_dir = os.path.join("data", "processed")
    db_path = os.path.join("data", "ecommerce.db")
    
    # Generate raw synthetic CSV files
    generator = EcommerceDataGenerator(seed=42)
    cust = generator.generate_customers(100)
    prod = generator.generate_products(30)
    ord_ = generator.generate_orders(1000, cust, prod)
    pay = generator.generate_payments(ord_)
    rev = generator.generate_reviews(300, cust, prod)
    
    generator.save_to_csv(cust, os.path.join(raw_dir, "customers.csv"))
    generator.save_to_csv(prod, os.path.join(raw_dir, "products.csv"))
    generator.save_to_csv(ord_, os.path.join(raw_dir, "orders.csv"))
    generator.save_to_csv(pay, os.path.join(raw_dir, "payments.csv"))
    generator.save_to_csv(rev, os.path.join(raw_dir, "reviews.csv"))

    # Instantiate and run pipeline orchestrator
    pipeline = EcommerceETLPipeline(
        raw_data_dir=raw_dir,
        processed_data_dir=processed_dir,
        db_path=db_path
    )
    
    result = pipeline.run()
    print("\nPipeline Result Summary:")
    print(f"  Status       : {result['status']}")
    print(f"  Row Counts   : {result['row_counts']}")
    print(f"  FK Violations: {result['fk_violations']}")

if __name__ == "__main__":
    main()
