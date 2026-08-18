"""
Unit tests for Milestone 8 & 9 Unified Platform CLI (EcommercePlatformCLI).
"""

import json
import os
import sys
from pathlib import Path
import pytest

# Ensure project root is on sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.cli import EcommercePlatformCLI


@pytest.fixture
def temp_platform_cli(tmp_path):
    """Fixture initializing EcommercePlatformCLI using isolated temporary directory."""
    return EcommercePlatformCLI(base_dir=str(tmp_path))


def test_cli_data_generation(temp_platform_cli: EcommercePlatformCLI):
    """Test data generation via CLI."""
    res = temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    assert res["customers"] == 10
    assert res["products"] == 5
    assert res["orders"] == 20
    assert (temp_platform_cli.raw_dir / "customers.csv").exists()
    assert (temp_platform_cli.raw_dir / "products.csv").exists()
    assert (temp_platform_cli.raw_dir / "orders.csv").exists()


def test_cli_quality_audit(temp_platform_cli: EcommercePlatformCLI):
    """Test data quality audit via CLI."""
    temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    report = temp_platform_cli.run_quality_audit()
    assert "summary" in report
    assert report["summary"]["overall_passed"] is True
    assert (temp_platform_cli.processed_dir / "data_quality_report.json").exists()


def test_cli_etl_pipeline(temp_platform_cli: EcommercePlatformCLI):
    """Test ETL pipeline execution via CLI."""
    temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    res = temp_platform_cli.run_etl_pipeline()
    assert res["status"] == "SUCCESS"
    assert temp_platform_cli.db_path.exists()


def test_cli_sql_analytics(temp_platform_cli: EcommercePlatformCLI):
    """Test SQL analytics execution via CLI."""
    temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    summary = temp_platform_cli.run_sql_analytics()
    assert "executive_kpis" in summary
    assert "reconciliation_discrepancies" in summary
    assert summary["reconciliation_discrepancies"] == 0


def test_cli_dag_orchestration(temp_platform_cli: EcommercePlatformCLI):
    """Test DAG orchestration via CLI."""
    db_file = Path("data/ecommerce.db")
    if db_file.exists():
        try:
            os.remove(db_file)
        except OSError:
            pass
    res = temp_platform_cli.run_dag_orchestration()
    assert res["passed"] is True
    assert res["total_tasks"] == 7


def test_cli_incremental_cdc(temp_platform_cli: EcommercePlatformCLI):
    """Test running incremental CDC via CLI."""
    temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    temp_platform_cli.run_etl_pipeline()
    res = temp_platform_cli.run_incremental_cdc()
    assert "INSERT" in res


def test_cli_observability(temp_platform_cli: EcommercePlatformCLI):
    """Test running observability engine via CLI."""
    temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    res = temp_platform_cli.run_observability()
    assert res["nodes"] == 3
    assert (temp_platform_cli.processed_dir / "platform_lineage.json").exists()
    assert (temp_platform_cli.processed_dir / "execution_telemetry.json").exists()


def test_cli_data_warehouse(temp_platform_cli: EcommercePlatformCLI):
    """Test running data warehouse ELT build via CLI."""
    temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    temp_platform_cli.run_etl_pipeline()
    res = temp_platform_cli.run_data_warehouse()
    assert res["status"] == "SUCCESS"
    assert res["fact_sales_records"] == 20
    assert temp_platform_cli.dw_path.exists()


def test_cli_summary_report(temp_platform_cli: EcommercePlatformCLI):
    """Test platform summary report generation via CLI."""
    temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    temp_platform_cli.run_etl_pipeline()
    summary = temp_platform_cli.generate_platform_summary_report()
    assert summary["platform_name"] == "E-Commerce Data Platform"
    assert summary["database_exists"] is True
    assert (temp_platform_cli.processed_dir / "platform_summary_report.json").exists()


def test_cli_main_argument_parsing(temp_platform_cli: EcommercePlatformCLI, capsys):
    """Test main CLI argument dispatcher."""
    # Test --report argument
    temp_platform_cli.run_data_generation(count_customers=10, count_products=5, count_orders=20)
    temp_platform_cli.run_etl_pipeline()
    temp_platform_cli.main(["--report"])
    captured = capsys.readouterr()
    assert "[CLI] Platform Summary Report written to" in captured.out
