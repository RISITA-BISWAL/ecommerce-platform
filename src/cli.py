"""
Unified Platform CLI & Reporting Module for E-Commerce Data Platform.

This module provides a command-line interface (CLI) and reporting summary
that unifies all data engineering capabilities built across Milestones 1–9:
- Data Generation (Milestone 1)
- Data Quality Audit (Milestone 2)
- Data Transformation & Database Loading (Milestones 3 & 4)
- SQL Analytics & Revenue Reconciliation (Milestone 5)
- Native DAG Orchestration (Milestone 6)
- PySpark Distributed Processing (Milestone 7)
- Unified CLI & Reporting (Milestone 8)
- Incremental CDC & Observability (Milestone 9)
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.agent import DataPlatformAgent
from src.analytics import EcommerceAnalytics
from src.database import EcommerceDatabase
from src.generator import EcommerceDataGenerator
from src.incremental import IncrementalCDCProcessor
from src.observability import PlatformObservabilityEngine
from src.orchestrator import build_ecommerce_dag
from src.pipeline import EcommerceETLPipeline
from src.validator import DataQualityAuditor
from src.warehouse import EcommerceDataWarehouse


class EcommercePlatformCLI:
    """Unified Command Line Interface for managing and executing the E-Commerce Data Platform."""

    def __init__(self, base_dir: Optional[str] = None):
        """Initialize directory configurations for raw, processed, database, and warehouse paths."""
        self.base_dir = Path(base_dir) if base_dir else project_root
        self.raw_dir = self.base_dir / "data" / "raw"
        self.processed_dir = self.base_dir / "data" / "processed"
        self.db_path = self.base_dir / "data" / "ecommerce.db"
        self.dw_path = self.base_dir / "data" / "ecommerce_dw.db"
        self.spark_processed_dir = self.base_dir / "data" / "spark_processed"

    def run_data_generation(
        self, count_customers: int = 100, count_products: int = 30, count_orders: int = 1000
    ) -> Dict[str, Any]:
        """Run Milestone 1 synthetic data generation."""
        print("[CLI] Running Synthetic Data Generation...")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        generator = EcommerceDataGenerator(seed=42)

        cust = generator.generate_customers(count_customers)
        prod = generator.generate_products(count_products)
        orders = generator.generate_orders(count_orders, cust, prod)
        pay = generator.generate_payments(orders)
        rev = generator.generate_reviews(300, cust, prod)

        generator.save_to_csv(cust, str(self.raw_dir / "customers.csv"))
        generator.save_to_csv(prod, str(self.raw_dir / "products.csv"))
        generator.save_to_csv(orders, str(self.raw_dir / "orders.csv"))
        generator.save_to_csv(pay, str(self.raw_dir / "payments.csv"))
        generator.save_to_csv(rev, str(self.raw_dir / "reviews.csv"))

        result = {
            "customers": len(cust),
            "products": len(prod),
            "orders": len(orders),
            "payments": len(pay),
            "reviews": len(rev),
        }
        print(f"[CLI] Generation complete: {result}")
        return result

    def run_quality_audit(self) -> Dict[str, Any]:
        """Run Milestone 2 data quality audit."""
        print("[CLI] Running Data Quality Audit...")
        auditor = DataQualityAuditor(raw_data_dir=str(self.raw_dir))
        report_path = str(self.processed_dir / "data_quality_report.json")
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        report = auditor.audit_datasets(report_output_path=report_path)
        print(f"[CLI] Quality Audit Overall Passed: {report['summary']['overall_passed']}")
        return report

    def run_etl_pipeline(self) -> Dict[str, Any]:
        """Run Milestone 3 & 4 ETL Pipeline."""
        print("[CLI] Running ETL Pipeline (Transform & Load)...")
        pipeline = EcommerceETLPipeline(
            raw_data_dir=str(self.raw_dir),
            processed_data_dir=str(self.processed_dir),
            db_path=str(self.db_path),
        )
        res = pipeline.run()
        print(f"[CLI] ETL Pipeline Result: Status={res['status']}")
        return res

    def run_sql_analytics(self) -> Dict[str, Any]:
        """Run Milestone 5 SQL Analytics."""
        print("[CLI] Running SQL Analytics & Reconciliation...")
        if not self.db_path.exists():
            print("[CLI] Database not found. Running ETL Pipeline first...")
            self.run_etl_pipeline()

        analytics = EcommerceAnalytics(db_path=str(self.db_path))
        kpis = analytics.get_executive_kpis().to_dict(orient="records")[0]
        reconcil = analytics.reconcile_revenue().to_dict(orient="records")

        summary = {
            "executive_kpis": kpis,
            "reconciliation_discrepancies": len(reconcil),
        }
        print(f"[CLI] SQL Analytics Complete. Executive KPIs: {kpis}")
        return summary

    def run_dag_orchestration(self) -> Dict[str, Any]:
        """Run Milestone 6 Native DAG Orchestrator."""
        print("[CLI] Running Native DAG Orchestrator...")
        dag = build_ecommerce_dag()
        res = dag.run_dag()
        print(f"[CLI] DAG Orchestration complete: Passed={res['passed']}")
        return res

    def run_spark_processor(self) -> Dict[str, Any]:
        """Run Milestone 7 PySpark Processing."""
        print("[CLI] Running PySpark Processing Subprocess...")
        venv_spark_python = project_root / ".venv_spark" / "Scripts" / "python.exe"
        cmd = [
            str(venv_spark_python) if venv_spark_python.exists() else sys.executable,
            "run_spark.py",
        ]
        proc = subprocess.run(cmd, cwd=str(project_root), capture_output=True, text=True)
        success = proc.returncode == 0
        print(f"[CLI] PySpark Execution Success: {success}")
        return {
            "success": success,
            "returncode": proc.returncode,
            "output_sample": proc.stdout[:300] if proc.stdout else proc.stderr[:300],
        }

    def run_incremental_cdc(self) -> Dict[str, Any]:
        """Run Milestone 9 Incremental CDC processing."""
        print("[CLI] Running Incremental CDC Merge...")
        if not self.db_path.exists():
            print("[CLI] Database not found. Running ETL Pipeline first...")
            self.run_etl_pipeline()

        cdc_processor = IncrementalCDCProcessor(primary_key="order_id")
        delta = [
            {
                "order_id": "ORD_CLI_INC_01",
                "customer_id": "CUST001",
                "product_id": "PROD001",
                "quantity": 3,
                "total_price": 149.99,
                "order_date": "2026-08-17 09:30:00",
                "status": "Completed",
            }
        ]
        summary = cdc_processor.upsert_sqlite_table(
            db_path=str(self.db_path),
            table_name="orders",
            incoming_delta=delta,
        )
        print(f"[CLI] Incremental CDC Merge Complete: {summary}")
        return summary

    def run_observability(self) -> Dict[str, Any]:
        """Run Milestone 9 Lineage and Observability Telemetry."""
        print("[CLI] Running Platform Observability Engine...")
        engine = PlatformObservabilityEngine(threshold=0.20)
        engine.add_lineage_node("raw_data", "csv_directory", {"path": str(self.raw_dir)})
        engine.add_lineage_node("processed_data", "processed_directory", {"path": str(self.processed_dir)})
        engine.add_lineage_node("sqlite_db", "database", {"path": str(self.db_path)})

        engine.add_lineage_edge("raw_data", "processed_data", "CLEAN_AND_STANDARDIZE")
        engine.add_lineage_edge("processed_data", "sqlite_db", "SQLITE_INGEST")

        engine.record_execution_telemetry("platform_cli_observability", duration_seconds=0.01, row_count=100)

        lineage_path = str(self.processed_dir / "platform_lineage.json")
        telemetry_path = str(self.processed_dir / "execution_telemetry.json")

        engine.export_lineage_json(lineage_path)
        engine.export_telemetry_json(telemetry_path)

        res = {
            "lineage_report": lineage_path,
            "telemetry_report": telemetry_path,
            "nodes": len(engine.nodes),
            "edges": len(engine.edges),
        }
        print(f"[CLI] Observability Complete: Generated lineage ({res['nodes']} nodes, {res['edges']} edges)")
        return res

    def run_data_warehouse(self) -> Dict[str, Any]:
        """Run Milestone 10 Data Warehouse ELT build & Star Schema population."""
        print("[CLI] Running Data Warehouse & Star Schema Builder...")
        dw = EcommerceDataWarehouse(db_path=str(self.db_path), dw_path=str(self.dw_path))
        summary = dw.build_full_warehouse()
        print(f"[CLI] Data Warehouse Build Complete: {summary['fact_sales_records']} Fact Sales loaded.")
        return summary

    def run_agent(self, query: Optional[str] = None, interactive: bool = False) -> str:
        """Run Milestone 11 Agentic Data Engineering Assistant."""
        print("[CLI] Launching Agentic Data Engineering Assistant...")
        agent = DataPlatformAgent(base_dir=str(self.base_dir))

        if interactive:
            print("============================================================")
            print("[Agentic Data Engineering Assistant - Milestone 11]")
            print("============================================================")
            print("Type your question in plain English, or type 'exit' to quit.\n")
            while True:
                try:
                    prompt = input("User > ").strip()
                    if not prompt:
                        continue
                    if prompt.lower() in ("exit", "quit", "q"):
                        print("Goodbye!")
                        break
                    resp = agent.ask(prompt)
                    print(f"\n[Agent Response]\n{resp}\n")
                    print("-" * 60)
                except (KeyboardInterrupt, EOFError):
                    print("\nGoodbye!")
                    break
            return "Interactive session closed."
        else:
            prompt = query if query else "What is the total revenue by category?"
            print(f"[CLI Query]: {prompt}\n")
            resp = agent.ask(prompt)
            print(resp)
            return resp

    def generate_platform_summary_report(self) -> Dict[str, Any]:
        """Generate comprehensive platform summary report."""
        print("[CLI] Generating Platform Summary Report...")
        summary: Dict[str, Any] = {
            "platform_name": "E-Commerce Data Platform",
            "raw_data_exists": self.raw_dir.exists(),
            "processed_data_exists": self.processed_dir.exists(),
            "database_exists": self.db_path.exists(),
            "warehouse_exists": self.dw_path.exists(),
            "spark_processed_exists": self.spark_processed_dir.exists(),
            "agent_available": True,
        }

        if self.db_path.exists():
            analytics = EcommerceAnalytics(db_path=str(self.db_path))
            summary["kpis"] = analytics.get_executive_kpis().to_dict(orient="records")[0]

        report_file = self.processed_dir / "platform_summary_report.json"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        print(f"[CLI] Platform Summary Report written to {report_file}")
        return summary

    def run_all(self) -> Dict[str, Any]:
        """Run full platform workflow end-to-end (Milestones 1 through 11)."""
        print("==================================================================")
        print("   STARTING UNIFIED E-COMMERCE PLATFORM END-TO-END EXECUTION")
        print("==================================================================\n")

        gen_res = self.run_data_generation()
        audit_res = self.run_quality_audit()
        etl_res = self.run_etl_pipeline()
        analytics_res = self.run_sql_analytics()
        dag_res = self.run_dag_orchestration()
        spark_res = self.run_spark_processor()
        cdc_res = self.run_incremental_cdc()
        obs_res = self.run_observability()
        dw_res = self.run_data_warehouse()
        agent_res = self.run_agent(query="What is the total revenue by category?")
        report_res = self.generate_platform_summary_report()

        print("\n==================================================================")
        print("   PLATFORM EXECUTION COMPLETED SUCCESSFULLY!")
        print("==================================================================")

        return {
            "generation": gen_res,
            "audit": audit_res,
            "etl": etl_res,
            "analytics": analytics_res,
            "dag": dag_res,
            "spark": spark_res,
            "cdc": cdc_res,
            "observability": obs_res,
            "warehouse": dw_res,
            "agent": agent_res,
            "report": report_res,
        }

    def main(self, args: Optional[list[str]] = None) -> None:
        """Parse CLI arguments and dispatch commands."""
        parser = argparse.ArgumentParser(
            description="Unified E-Commerce Data Platform CLI (Milestones 1–11)"
        )
        parser.add_argument("--generate", action="store_true", help="Run synthetic data generation")
        parser.add_argument("--audit", action="store_true", help="Run data quality audit")
        parser.add_argument("--pipeline", action="store_true", help="Run ETL pipeline")
        parser.add_argument("--analytics", action="store_true", help="Run SQL analytics & KPIs")
        parser.add_argument("--orchestrate", action="store_true", help="Run DAG workflow orchestrator")
        parser.add_argument("--spark", action="store_true", help="Run PySpark distributed processing")
        parser.add_argument("--incremental", "--cdc", action="store_true", help="Run incremental CDC batch merge")
        parser.add_argument("--observability", "--lineage", action="store_true", help="Generate platform lineage & telemetry")
        parser.add_argument("--warehouse", action="store_true", help="Build Star Schema Data Warehouse")
        parser.add_argument("--agent", nargs="?", const="", type=str, help="Run Agentic Data Engineering Assistant with query")
        parser.add_argument("--agent-interactive", action="store_true", help="Run Agentic Data Engineering Assistant in interactive mode")
        parser.add_argument("--report", action="store_true", help="Generate platform summary report")
        parser.add_argument("--all", action="store_true", help="Run full platform workflow end-to-end")

        parsed_args = parser.parse_args(args)

        if parsed_args.all:
            self.run_all()
        elif parsed_args.generate:
            self.run_data_generation()
        elif parsed_args.audit:
            self.run_quality_audit()
        elif parsed_args.pipeline:
            self.run_etl_pipeline()
        elif parsed_args.analytics:
            self.run_sql_analytics()
        elif parsed_args.orchestrate:
            self.run_dag_orchestration()
        elif parsed_args.spark:
            self.run_spark_processor()
        elif parsed_args.incremental:
            self.run_incremental_cdc()
        elif parsed_args.observability:
            self.run_observability()
        elif parsed_args.warehouse:
            self.run_data_warehouse()
        elif parsed_args.agent_interactive or parsed_args.agent is not None:
            query = parsed_args.agent if parsed_args.agent else None
            is_interactive = parsed_args.agent_interactive or (parsed_args.agent == "")
            self.run_agent(query=query, interactive=is_interactive)
        elif parsed_args.report:
            self.generate_platform_summary_report()
        else:
            parser.print_help()


if __name__ == "__main__":
    cli = EcommercePlatformCLI()
    cli.main()

