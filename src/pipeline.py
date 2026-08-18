import os
from src.validator import DataQualityAuditor
from src.transformer import DataTransformer
from src.database import EcommerceDatabase

class ETLValidationError(Exception):
    """Raised when raw data fails quality validation rules."""
    pass

class ETLDbError(Exception):
    """Raised when database loading or relational constraint checks fail."""
    pass

class EcommerceETLPipeline:
    """
    Production-grade ETL Pipeline Orchestrator for E-Commerce Data Engineering.
    Manages sequential execution: Validation -> Quality Report -> Transformation -> DB Loading -> DB Verification.
    """
    def __init__(
        self,
        raw_data_dir: str = os.path.join("data", "raw"),
        processed_data_dir: str = os.path.join("data", "processed"),
        db_path: str = os.path.join("data", "ecommerce.db")
    ):
        self.raw_data_dir = raw_data_dir
        self.processed_data_dir = processed_data_dir
        self.db_path = db_path
        self.report_path = os.path.join(self.processed_data_dir, "data_quality_report.json")

    def run(self) -> dict:
        """
        Executes the end-to-end pipeline with strict error stopping and validation enforcement.
        Returns a summary report dict on success.
        Raises ETLValidationError if validation fails.
        Raises ETLDbError if database verification fails.
        """
        print("[INFO] Starting End-to-End E-Commerce ETL Pipeline...")

        # 1. Validation & Quality Audit
        print(f" [1/4] Auditing raw data quality in '{self.raw_data_dir}'...")
        auditor = DataQualityAuditor(raw_data_dir=self.raw_data_dir)
        report = auditor.audit_datasets(report_output_path=self.report_path)
        
        overall_passed = report["summary"]["overall_passed"]
        issues_count = report["summary"]["total_issues_found"]
        
        print(f"      Quality Audit Complete: Report saved to '{self.report_path}'")

        # 2. Strict Validation Check: STOP PIPELINE if validation failed
        if not overall_passed:
            print(f" ❌ CRITICAL FAILURE: Data Quality Audit Failed with {issues_count} issues!")
            print("      Pipeline HALTED before transformation and database loading.")
            raise ETLValidationError(
                f"Data quality audit failed with {issues_count} issues. "
                f"Inspect report at '{self.report_path}' for details."
            )

        print(" [1/4] Quality Audit PASSED (0 issues found). Proceeding to Transformation...")

        # 3. Data Transformation & Cleaning
        print(f" [2/4] Executing Data Transformations -> Output to '{self.processed_data_dir}'...")
        transformer = DataTransformer(raw_data_dir=self.raw_data_dir, processed_data_dir=self.processed_data_dir)
        processed_dfs = transformer.transform_datasets()
        print("      Transformations & feature engineering complete.")

        # 4. Database Loading via Explicit UPSERT
        print(f" [3/4] Loading processed data into SQLite database: '{self.db_path}'...")
        db = EcommerceDatabase(db_path=self.db_path)
        count_verification = db.load_csv_data(raw_data_dir=self.processed_data_dir)
        
        for table, meta in count_verification.items():
            print(f"      - {table:10s}: CSV count = {meta['csv_count']}, DB count = {meta['db_count']} (Match: {meta['matched']})")
            if not meta["matched"]:
                raise ETLDbError(f"Database row count mismatch in table '{table}': CSV={meta['csv_count']} vs DB={meta['db_count']}")

        # 5. Database Relational Verification (PRAGMA foreign_key_check)
        print(" [4/4] Executing PRAGMA foreign_key_check in SQLite...")
        violations = db.verify_foreign_keys()
        if violations:
            print(f" ❌ CRITICAL FAILURE: Detected {len(violations)} foreign key violations in SQLite!")
            raise ETLDbError(f"SQLite foreign key check failed with violations: {violations}")

        print("      PRAGMA foreign_key_check: 0 violations.")
        print("\n[SUCCESS] End-to-End ETL Pipeline Executed Successfully!")

        return {
            "status": "SUCCESS",
            "quality_report_summary": report["summary"],
            "row_counts": db.get_row_counts(),
            "fk_violations": len(violations)
        }
