import json
import os
import re
import pandas as pd

class DataQualityAuditor:
    """
    Performs deterministic data-quality audits across raw e-commerce CSV datasets.
    Generates a structured, machine-readable report containing issue counts per rule.
    """
    def __init__(self, raw_data_dir: str = os.path.join("data", "raw")):
        self.raw_data_dir = raw_data_dir
        self.allowed_order_statuses = {"Completed", "Pending", "Cancelled", "Shipped"}
        self.allowed_payment_statuses = {"Completed", "Pending", "Failed", "Refunded"}

    @staticmethod
    def _is_valid_iso_date(val: str) -> bool:
        """Helper to verify if a string matches YYYY-MM-DD HH:MM:SS date format."""
        if not isinstance(val, str):
            return False
        pattern = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
        if not re.match(pattern, val):
            return False
        try:
            pd.to_datetime(val, format="%Y-%m-%d %H:%M:%S")
            return True
        except Exception:
            return False

    def audit_datasets(self, report_output_path: str = os.path.join("data", "processed", "data_quality_report.json")) -> dict:
        """
        Audits customers, products, orders, payments, and reviews CSV datasets.
        Returns a comprehensive report dict and writes report_output_path JSON file.
        """
        # Ensure datasets exist
        files = {
            "customers": "customers.csv",
            "products": "products.csv",
            "orders": "orders.csv",
            "payments": "payments.csv",
            "reviews": "reviews.csv"
        }
        
        dfs = {}
        for key, fname in files.items():
            fpath = os.path.join(self.raw_data_dir, fname)
            if not os.path.exists(fpath):
                raise FileNotFoundError(f"Raw CSV dataset missing for audit: {fpath}")
            dfs[key] = pd.read_csv(fpath)

        report = {
            "summary": {"total_datasets": 5, "overall_passed": True, "total_issues_found": 0},
            "datasets": {}
        }

        # Sets of Primary IDs for FK checks
        cust_ids = set(dfs["customers"]["customer_id"].astype(str)) if "customer_id" in dfs["customers"] else set()
        prod_ids = set(dfs["products"]["product_id"].astype(str)) if "product_id" in dfs["products"] else set()
        ord_ids = set(dfs["orders"]["order_id"].astype(str)) if "order_id" in dfs["orders"] else set()

        # Audit each dataset
        for name, df in dfs.items():
            ds_report = {
                "dataset_name": name,
                "rows_checked": len(df),
                "missing_value_count": 0,
                "duplicate_id_count": 0,
                "invalid_value_counts": {},
                "foreign_key_violations": 0,
                "date_errors": 0,
                "status_errors": 0,
                "passed": True
            }

            # 1. Missing Values
            null_count = int(df.isnull().sum().sum())
            ds_report["missing_value_count"] = null_count

            # 2. Duplicate Primary IDs
            primary_id_map = {
                "customers": "customer_id",
                "products": "product_id",
                "orders": "order_id",
                "payments": "payment_id",
                "reviews": "review_id"
            }
            pk_col = primary_id_map.get(name)
            if pk_col and pk_col in df.columns:
                dups = int(df[pk_col].duplicated().sum())
                ds_report["duplicate_id_count"] = dups

            # 3. Numeric & Value Constraints
            invalid_vals = {}
            if name == "products":
                invalid_prices = int((pd.to_numeric(df["price"], errors="coerce") <= 0).sum())
                invalid_stock = int((pd.to_numeric(df["stock"], errors="coerce") < 0).sum())
                invalid_vals["invalid_prices"] = invalid_prices
                invalid_vals["invalid_stock"] = invalid_stock
            elif name == "orders":
                invalid_qty = int((pd.to_numeric(df["quantity"], errors="coerce") <= 0).sum())
                invalid_total_price = int((pd.to_numeric(df["total_price"], errors="coerce") <= 0).sum())
                invalid_vals["invalid_quantities"] = invalid_qty
                invalid_vals["invalid_total_prices"] = invalid_total_price
            elif name == "payments":
                invalid_amount = int((pd.to_numeric(df["amount"], errors="coerce") <= 0).sum())
                invalid_vals["invalid_amounts"] = invalid_amount
            elif name == "reviews":
                invalid_ratings = int((~pd.to_numeric(df["rating"], errors="coerce").between(1, 5)).sum())
                invalid_vals["invalid_ratings"] = invalid_ratings

            ds_report["invalid_value_counts"] = invalid_vals

            # 4. Foreign Key Violations
            fk_violations = 0
            if name == "orders":
                fk_violations += int((~df["customer_id"].astype(str).isin(cust_ids)).sum())
                fk_violations += int((~df["product_id"].astype(str).isin(prod_ids)).sum())
            elif name == "payments":
                fk_violations += int((~df["order_id"].astype(str).isin(ord_ids)).sum())
            elif name == "reviews":
                fk_violations += int((~df["customer_id"].astype(str).isin(cust_ids)).sum())
                fk_violations += int((~df["product_id"].astype(str).isin(prod_ids)).sum())

            ds_report["foreign_key_violations"] = fk_violations

            # 5. Date Errors
            date_cols = {"customers": ["created_at"], "orders": ["order_date"], "payments": ["payment_date"], "reviews": ["review_date"]}.get(name, [])
            date_errs = 0
            for dcol in date_cols:
                if dcol in df.columns:
                    date_errs += int((~df[dcol].apply(self._is_valid_iso_date)).sum())
            ds_report["date_errors"] = date_errs

            # 6. Status Errors
            status_errs = 0
            if name == "orders" and "status" in df.columns:
                status_errs = int((~df["status"].isin(self.allowed_order_statuses)).sum())
            elif name == "payments" and "status" in df.columns:
                status_errs = int((~df["status"].isin(self.allowed_payment_statuses)).sum())
            ds_report["status_errors"] = status_errs

            # Determine pass/fail for this dataset
            total_issues = (
                ds_report["missing_value_count"] +
                ds_report["duplicate_id_count"] +
                sum(ds_report["invalid_value_counts"].values()) +
                ds_report["foreign_key_violations"] +
                ds_report["date_errors"] +
                ds_report["status_errors"]
            )
            
            if total_issues > 0:
                ds_report["passed"] = False
                report["summary"]["overall_passed"] = False

            report["summary"]["total_issues_found"] += total_issues
            report["datasets"][name] = ds_report

        # Write machine-readable JSON quality report
        if report_output_path:
            os.makedirs(os.path.dirname(report_output_path), exist_ok=True)
            with open(report_output_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)

        return report
