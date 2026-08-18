import os
import pandas as pd

class DataTransformer:
    """
    Applies deterministic data cleaning, standardization, revenue recalculation,
    and analytical feature engineering using pandas. Outputs processed datasets to data/processed/.
    """
    def __init__(self, raw_data_dir: str = os.path.join("data", "raw"), processed_data_dir: str = os.path.join("data", "processed")):
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir

    def transform_datasets(self) -> dict:
        """
        Reads raw CSV files, applies transformations, exports processed CSVs,
        and returns a dictionary of processed DataFrames.
        """
        # Ensure target processed directory exists
        os.makedirs(self.processed_data_dir, exist_ok=True)

        # 1. Load Raw Datasets
        customers = pd.read_csv(os.path.join(self.raw_data_dir, "customers.csv"))
        products = pd.read_csv(os.path.join(self.raw_data_dir, "products.csv"))
        orders = pd.read_csv(os.path.join(self.raw_data_dir, "orders.csv"))
        payments = pd.read_csv(os.path.join(self.raw_data_dir, "payments.csv"))
        reviews = pd.read_csv(os.path.join(self.raw_data_dir, "reviews.csv"))

        # 2. Text Standardization (Trim Whitespace & Lowercase Emails)
        for df in [customers, products, orders, payments, reviews]:
            str_cols = df.select_dtypes(include=["object", "str", "string"]).columns
            for col in str_cols:
                df[col] = df[col].astype(str).str.strip()


        customers["email"] = customers["email"].str.lower()

        # 3. Date Standardization (Parse to standard ISO %Y-%m-%d %H:%M:%S format)
        customers["created_at"] = pd.to_datetime(customers["created_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        orders["order_date"] = pd.to_datetime(orders["order_date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        payments["payment_date"] = pd.to_datetime(payments["payment_date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        reviews["review_date"] = pd.to_datetime(reviews["review_date"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        # 4. Order Revenue Verification & Recalculation
        # Merge orders with products to get authoritative unit price
        orders_merged = orders.merge(products[["product_id", "price"]], on="product_id", how="left", suffixes=("", "_prod"))
        orders["calculated_total_price"] = (orders_merged["quantity"] * orders_merged["price"]).round(2)
        orders["has_price_discrepancy"] = (orders["total_price"] != orders["calculated_total_price"])
        # Update total_price to calculated revenue for full accuracy
        orders["total_price"] = orders["calculated_total_price"]

        # 5. Analytical Feature Engineering
        # Orders Features: order_year, order_month, order_dayofweek
        order_dt = pd.to_datetime(orders["order_date"])
        orders["order_year"] = order_dt.dt.year
        orders["order_month"] = order_dt.dt.month
        orders["order_dayofweek"] = order_dt.dt.day_name()

        # Customers Feature: customer_tenure_days (days since signup relative to max order date)
        latest_order_dt = order_dt.max()
        cust_dt = pd.to_datetime(customers["created_at"])
        customers["customer_tenure_days"] = (latest_order_dt - cust_dt).dt.days

        # Reviews Feature: rating_category
        def categorize_rating(r):
            if r >= 4:
                return "Positive"
            elif r == 3:
                return "Neutral"
            else:
                return "Negative"

        reviews["rating_category"] = reviews["rating"].apply(categorize_rating)

        # 6. Save Processed Datasets (Leaving raw datasets completely unchanged)
        processed_dfs = {
            "customers": customers,
            "products": products,
            "orders": orders,
            "payments": payments,
            "reviews": reviews
        }

        for name, df in processed_dfs.items():
            out_file = os.path.join(self.processed_data_dir, f"{name}.csv")
            df.to_csv(out_file, index=False)

        return processed_dfs
