"""
PySpark Processing Runner for E-Commerce Data Platform.

This script executes distributed data processing, analytical transformations,
and Parquet dataset exports using PySpark.

Key Features & Operations Demonstrated:
1. SparkSession Management: Lifecycle control with clean try/finally shutdown.
2. Explicit Schema Reading: Reading CSV datasets with pre-defined StructType schemas.
3. PySpark Transformations:
   - select(): Selecting specific columns.
   - filter(): Filtering rows by status (e.g., status == 'Completed').
   - withColumn(): Creating or modifying derived columns.
   - groupBy(): Grouping datasets by dimensions (e.g., product_id, category).
   - agg(): Performing aggregate functions like sum() and round().
   - orderBy(): Ordering results by metric values descending.
4. PySpark Actions:
   - count(): Counting total records in distributed DataFrames.
   - printSchema(): Printing structural schemas of DataFrames.
   - show(): Displaying tabular results to stdout.
   - write.parquet(): Exporting columnar Parquet files for downstream analytics.
"""

import os
import sys
from pathlib import Path
import pyspark

# Add project root directory to Python path to ensure module resolution
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Set HADOOP_HOME to point to project's data/hadoop directory for Windows compatibility
hadoop_home = project_root / "data" / "hadoop"
if hadoop_home.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)

from src.spark_processor import EcommerceSparkProcessor


def main():
    """Main execution function for PySpark e-commerce data processing pipeline."""
    # Define file paths using pathlib
    raw_data_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "spark_processed"

    customers_csv = str(raw_data_dir / "customers.csv")
    products_csv = str(raw_data_dir / "products.csv")
    orders_csv = str(raw_data_dir / "orders.csv")
    payments_csv = str(raw_data_dir / "payments.csv")
    reviews_csv = str(raw_data_dir / "reviews.csv")

    # Destination Parquet paths
    product_summary_parquet = str(processed_dir / "product_sales_summary")
    category_revenue_parquet = str(processed_dir / "category_revenue")

    # Initialize EcommerceSparkProcessor instance
    processor = EcommerceSparkProcessor(
        app_name="EcommerceSparkPipeline",
        master="local[*]"
    )

    try:
        # Create and start SparkSession
        spark = processor.create_spark_session()

        print("=" * 60)
        print("        E-COMMERCE PYSPARK PROCESSING")
        print("=" * 60)
        print(f"Python Version:  {sys.version.split()[0]}")
        print(f"PySpark Version: {pyspark.__version__}")
        print(f"Spark Version:   {spark.version}")
        print(f"Spark Master:    {spark.sparkContext.master}\n")

        # Read the 5 raw CSV datasets using explicit StructType schemas
        print("--- LOADING DATASETS & SCHEMA INSPECTION ---")

        print("\n[Dataset 1: Customers]")
        customers_df = processor.read_customers(customers_csv)
        customers_df.printSchema()
        customers_count = customers_df.count()
        print(f"Row count: {customers_count}")

        print("\n[Dataset 2: Products]")
        products_df = processor.read_products(products_csv)
        products_df.printSchema()
        products_count = products_df.count()
        print(f"Row count: {products_count}")

        print("\n[Dataset 3: Orders]")
        orders_df = processor.read_orders(orders_csv)
        orders_df.printSchema()
        orders_count = orders_df.count()
        print(f"Row count: {orders_count}")

        print("\n[Dataset 4: Payments]")
        payments_df = processor.read_payments(payments_csv)
        payments_df.printSchema()
        payments_count = payments_df.count()
        print(f"Row count: {payments_count}")

        print("\n[Dataset 5: Reviews]")
        reviews_df = processor.read_reviews(reviews_csv)
        reviews_df.printSchema()
        reviews_count = reviews_df.count()
        print(f"Row count: {reviews_count}")

        print("\n" + "=" * 60)
        print("--- DATASET COUNTS ---")
        print(f"Customers: {customers_count}")
        print(f"Products:  {products_count}")
        print(f"Orders:    {orders_count}")
        print(f"Payments:  {payments_count}")
        print(f"Reviews:   {reviews_count}")

        # Compute Product Sales Summary analytics using PySpark transformations
        print("\n--- PRODUCT SALES SUMMARY ---")
        product_sales_summary_df = processor.get_product_sales_summary(
            products_df=products_df,
            orders_df=orders_df
        )
        product_sales_summary_df.show(truncate=False)

        # Compute Category Revenue analytics using PySpark transformations
        print("\n--- CATEGORY REVENUE ---")
        category_revenue_df = processor.get_category_revenue(
            products_df=products_df,
            orders_df=orders_df
        )
        category_revenue_df.show(truncate=False)

        # Export analytics DataFrames to Parquet format
        print("\n--- PARQUET EXPORT ---")
        processor.export_to_parquet(
            product_sales_summary_df,
            output_path=product_summary_parquet,
            mode="overwrite"
        )
        print("Product sales summary exported successfully.")

        processor.export_to_parquet(
            category_revenue_df,
            output_path=category_revenue_parquet,
            mode="overwrite"
        )
        print("Category revenue exported successfully.")

        print("\n[SUCCESS] PySpark processing completed successfully.")

    finally:
        # Cleanly stop SparkSession in finally block
        processor.stop_spark_session()
        print("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()
