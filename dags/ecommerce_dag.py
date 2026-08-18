"""
==============================================================================
APACHE AIRFLOW REFERENCE DAG — CONCEPTUAL LEARNING ONLY
==============================================================================
NOTE: Apache Airflow does not natively run on Windows without WSL2/Docker,
and Python 3.13 support is unreleased across many Airflow providers.

This DAG file is provided for learning purposes to demonstrate how the existing
Python platform components (src/generator.py, src/validator.py, src/transformer.py,
src/database.py, src/analytics.py) map to Apache Airflow PythonOperators and task dependencies.

Execution in this project is handled by native Windows orchestrator: src/orchestrator.py
==============================================================================
"""

from datetime import datetime, timedelta

# Airflow imports (Conceptual reference)
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    DAG = object
    PythonOperator = object

# Import existing platform components (Zero business logic duplication)
from src.generator import EcommerceDataGenerator
from src.validator import DataQualityAuditor
from src.transformer import DataTransformer
from src.database import EcommerceDatabase
from src.analytics import EcommerceAnalytics

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
}

# Task wrapper functions executing existing codebase components
def run_generator():
    gen = EcommerceDataGenerator(seed=42)
    gen.save_to_csv(gen.generate_customers(100), "data/raw/customers.csv")
    gen.save_to_csv(gen.generate_products(30), "data/raw/products.csv")
    gen.save_to_csv(gen.generate_orders(1000), "data/raw/orders.csv")

def run_validator():
    auditor = DataQualityAuditor(raw_data_dir="data/raw")
    rep = auditor.audit_datasets(report_output_path="data/processed/data_quality_report.json")
    if not rep["summary"]["overall_passed"]:
        raise ValueError("Data Quality Audit Failed")

def run_transformer():
    transformer = DataTransformer(raw_data_dir="data/raw", processed_data_dir="data/processed")
    transformer.transform_datasets()

def run_db_loader():
    db = EcommerceDatabase(db_path="data/ecommerce.db")
    db.load_csv_data(raw_data_dir="data/processed")

def run_db_verifier():
    db = EcommerceDatabase(db_path="data/ecommerce.db")
    violations = db.verify_foreign_keys()
    if violations:
        raise ValueError(f"FK violations: {violations}")

def run_analytics():
    analytics = EcommerceAnalytics(db_path="data/ecommerce.db")
    reconciliation = analytics.reconcile_revenue()
    if len(reconciliation) > 0:
        raise ValueError("Revenue discrepancies detected")

def export_reports():
    analytics = EcommerceAnalytics(db_path="data/ecommerce.db")
    _ = analytics.get_executive_kpis()

# Define Airflow DAG (Only if Airflow is installed in environment)
if AIRFLOW_AVAILABLE:
    with DAG(
        'ecommerce_platform_pipeline',
        default_args=default_args,
        description='An educational agentic e-commerce data engineering pipeline DAG',
        schedule_interval='@daily',
        catchup=False
    ) as dag:

        t1 = PythonOperator(task_id='generate_raw_data', python_callable=run_generator)
        t2 = PythonOperator(task_id='validate_raw_data', python_callable=run_validator)
        t3 = PythonOperator(task_id='transform_data', python_callable=run_transformer)
        t4 = PythonOperator(task_id='load_sqlite_db', python_callable=run_db_loader)
        t5 = PythonOperator(task_id='verify_database', python_callable=run_db_verifier)
        t6 = PythonOperator(task_id='run_sql_analytics', python_callable=run_analytics)
        t7 = PythonOperator(task_id='export_analytics_reports', python_callable=export_reports)

        # Airflow Task Dependency Graph
        t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
