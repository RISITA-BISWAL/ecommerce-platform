"""
Agentic Data Engineering Assistant for E-Commerce Data Platform (Milestone 11).

This module provides a natural-language query interface, intent router, tool registry,
and read-only SQL guardrail engine for interacting with the platform capabilities built
across Milestones 1–10.
"""

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Project root resolution
project_root = Path(__file__).resolve().parent.parent

from src.analytics import EcommerceAnalytics
from src.incremental import IncrementalCDCProcessor
from src.observability import PlatformObservabilityEngine
from src.validator import DataQualityAuditor
from src.warehouse import EcommerceDataWarehouse


class SQLGuardrailError(Exception):
    """Raised when a query violates read-only or SQL safety guardrails."""

    pass


class SQLGuardrailEngine:
    """Enforces read-only analytics queries and prevents unsafe SQL execution."""

    PROHIBITED_KEYWORDS = {
        "DROP",
        "DELETE",
        "UPDATE",
        "INSERT",
        "ALTER",
        "TRUNCATE",
        "CREATE",
        "REPLACE",
        "PRAGMA",
        "ATTACH",
        "DETACH",
        "VACUUM",
        "EXEC",
        "GRANT",
        "REVOKE",
    }

    @classmethod
    def validate_sql(cls, query: str) -> str:
        """Validate and sanitize a SQL query to ensure it is strictly read-only and single-statement."""
        cleaned_query = query.strip()
        if not cleaned_query:
            raise SQLGuardrailError("Empty query provided.")

        # Check for multiple stacked statements separated by semicolon
        # Allow a single trailing semicolon
        stripped_semicolon = cleaned_query.rstrip(";").strip()
        if ";" in stripped_semicolon:
            raise SQLGuardrailError("Multiple SQL statements separated by ';' are prohibited.")

        # Normalize words for keyword checks
        tokens = re.findall(r"\b[A-Za-z_]+\b", stripped_semicolon.upper())
        for token in tokens:
            if token in cls.PROHIBITED_KEYWORDS:
                raise SQLGuardrailError(
                    f"Query rejected: Prohibited keyword '{token}' detected in read-only mode."
                )

        if not tokens or tokens[0] not in ("SELECT", "WITH", "EXPLAIN"):
            raise SQLGuardrailError("Only SELECT, WITH, or EXPLAIN statements are allowed.")

        # Append default LIMIT 100 if SELECT statement without explicit LIMIT
        if "LIMIT" not in tokens:
            stripped_semicolon += " LIMIT 100"

        return stripped_semicolon

    @classmethod
    def execute_safe_query(cls, db_path: str, query: str) -> List[Dict[str, Any]]:
        """Execute a validated query against a SQLite database using URI read-only mode."""
        safe_sql = cls.validate_sql(query)
        db_file = Path(db_path)
        if not db_file.exists():
            raise FileNotFoundError(f"Database file not found at: {db_path}")

        # Open SQLite handle using read-only URI mode
        uri_path = f"file:{db_file.resolve().as_posix()}?mode=ro"
        conn = sqlite3.connect(uri_path, uri=True)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            cursor.execute(safe_sql)
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


class DataPlatformAgent:
    """Agentic Data Engineering Assistant for interactive platform querying."""

    def __init__(self, base_dir: Optional[str] = None):
        """Initialize directory paths and engine bindings."""
        self.base_dir = Path(base_dir) if base_dir else project_root
        self.db_path = self.base_dir / "data" / "ecommerce.db"
        self.dw_path = self.base_dir / "data" / "ecommerce_dw.db"
        self.raw_dir = self.base_dir / "data" / "raw"
        self.processed_dir = self.base_dir / "data" / "processed"
        self.spark_processed_dir = self.base_dir / "data" / "spark_processed"

        # Register tool handlers
        self.tools = {
            "tool_query_analytics": self.tool_query_analytics,
            "tool_query_warehouse": self.tool_query_warehouse,
            "tool_get_data_quality": self.tool_get_data_quality,
            "tool_get_lineage": self.tool_get_lineage,
            "tool_get_drift_metrics": self.tool_get_drift_metrics,
            "tool_get_execution_telemetry": self.tool_get_execution_telemetry,
            "tool_get_cdc_summary": self.tool_get_cdc_summary,
            "tool_get_spark_summary": self.tool_get_spark_summary,
        }

    # --- TOOL DEFINITIONS ---

    def tool_query_analytics(self, query_type: str = "kpi", custom_sql: Optional[str] = None) -> Dict[str, Any]:
        """Query operational database (ecommerce.db) via analytics engine or safe ad-hoc SQL."""
        if not self.db_path.exists():
            return {"error": "Operational database not found. Please run `run_platform.py --pipeline` first."}

        if custom_sql:
            try:
                results = SQLGuardrailEngine.execute_safe_query(str(self.db_path), custom_sql)
                return {"type": "custom_sql", "rows": results}
            except Exception as e:
                return {"error": str(e)}

        analytics = EcommerceAnalytics(db_path=str(self.db_path))
        if query_type == "kpi":
            df = analytics.get_executive_kpis()
            return {"type": "kpi", "rows": df.to_dict(orient="records")}
        elif query_type == "revenue_reconciliation":
            df = analytics.reconcile_revenue()
            return {"type": "revenue_reconciliation", "rows": df.to_dict(orient="records")}
        elif query_type == "payment_distribution":
            df = analytics.get_payment_method_distribution()
            return {"type": "payment_distribution", "rows": df.to_dict(orient="records")}
        elif query_type == "customer_orders":
            df = analytics.get_customer_order_frequency()
            return {"type": "customer_orders", "rows": df.to_dict(orient="records")}
        else:
            return {"error": f"Unknown analytics query_type: {query_type}"}

    def tool_query_warehouse(self, report_type: str = "category_revenue") -> Dict[str, Any]:
        """Query Star Schema Data Warehouse (ecommerce_dw.db) for dimensional OLAP summaries."""
        if not self.dw_path.exists():
            return {"error": "Data warehouse not found. Please run `run_platform.py --warehouse` first."}

        dw = EcommerceDataWarehouse(db_path=str(self.db_path), dw_path=str(self.dw_path))
        if report_type in ("category_revenue", "category"):
            df = dw.get_category_revenue_drilldown()
            return {"type": "dw_category_revenue", "rows": df.to_dict(orient="records")}
        elif report_type in ("weekend_vs_weekday", "weekend"):
            df = dw.get_weekend_sales_performance()
            return {"type": "dw_weekend_vs_weekday", "rows": df.to_dict(orient="records")}
        elif report_type in ("top_customers", "customer_spend"):
            dw_conn = dw.get_dw_connection()
            query = """
                SELECT 
                    c.name,
                    c.email,
                    COUNT(DISTINCT f.order_id) AS total_orders,
                    ROUND(SUM(f.total_price), 2) AS total_spend
                FROM fact_sales f
                JOIN dim_customer c ON f.customer_key = c.customer_key
                GROUP BY c.customer_key
                ORDER BY total_spend DESC
                LIMIT 5;
            """
            import pandas as pd
            df = pd.read_sql_query(query, dw_conn)
            dw_conn.close()
            return {"type": "dw_top_customers", "rows": df.to_dict(orient="records")}
        else:
            return {"error": f"Unknown warehouse report_type: {report_type}"}

    def tool_get_data_quality(self) -> Dict[str, Any]:
        """Retrieve data quality audit report."""
        report_file = self.processed_dir / "data_quality_report.json"
        if report_file.exists():
            with open(report_file, "r", encoding="utf-8") as f:
                report = json.load(f)
            return {"type": "data_quality", "report": report}

        if self.raw_dir.exists():
            auditor = DataQualityAuditor(raw_data_dir=str(self.raw_dir))
            report = auditor.audit_datasets()
            return {"type": "data_quality", "report": report}

        return {"error": "Raw data directory not found. Please run `run_platform.py --generate` first."}

    def tool_get_lineage(self) -> Dict[str, Any]:
        """Retrieve Directed Lineage Graph."""
        lineage_file = self.processed_dir / "platform_lineage.json"
        if lineage_file.exists():
            with open(lineage_file, "r", encoding="utf-8") as f:
                lineage = json.load(f)
            return {"type": "lineage", "lineage": lineage}

        obs = PlatformObservabilityEngine()
        obs.add_lineage_node("raw_data", "csv_directory", {"path": str(self.raw_dir)})
        obs.add_lineage_node("processed_data", "processed_directory", {"path": str(self.processed_dir)})
        obs.add_lineage_node("sqlite_db", "database", {"path": str(self.db_path)})
        obs.add_lineage_edge("raw_data", "processed_data", "CLEAN_AND_STANDARDIZE")
        obs.add_lineage_edge("processed_data", "sqlite_db", "SQLITE_INGEST")
        return {"type": "lineage", "lineage": {"nodes": list(obs.nodes.values()), "edges": obs.edges}}

    def tool_get_drift_metrics(self) -> Dict[str, Any]:
        """Retrieve numerical data drift & schema drift metrics."""
        obs = PlatformObservabilityEngine(threshold=0.20)
        baseline = 1000.0
        current = 1050.0
        drift_report = obs.detect_data_drift(baseline_metric=baseline, current_metric=current)
        return {"type": "drift", "drift_report": drift_report}

    def tool_get_execution_telemetry(self) -> Dict[str, Any]:
        """Retrieve execution telemetry and task runtime statistics."""
        telemetry_file = self.processed_dir / "execution_telemetry.json"
        if telemetry_file.exists():
            with open(telemetry_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {"type": "telemetry", "telemetry": data}
        return {"error": "Execution telemetry not found. Please run `run_platform.py --observability` first."}

    def tool_get_cdc_summary(self) -> Dict[str, Any]:
        """Retrieve Change Data Capture status & delta classification summary."""
        cdc_processor = IncrementalCDCProcessor(primary_key="order_id")
        return {
            "type": "cdc",
            "status": "Active",
            "processor": cdc_processor.__class__.__name__,
            "primary_key": cdc_processor.primary_key,
            "supported_operations": ["INSERT", "UPDATE", "NO_CHANGE"],
            "target_database": str(self.db_path),
        }

    def tool_get_spark_summary(self) -> Dict[str, Any]:
        """Retrieve PySpark Parquet data lake asset summary."""
        cat_dir = self.spark_processed_dir / "category_revenue"
        prod_dir = self.spark_processed_dir / "product_sales_summary"

        return {
            "type": "spark",
            "spark_processed_exists": self.spark_processed_dir.exists(),
            "category_revenue_parquet_exists": cat_dir.exists(),
            "product_sales_summary_parquet_exists": prod_dir.exists(),
            "output_directory": str(self.spark_processed_dir),
        }

    # --- INTENT ROUTER ---

    def parse_intent(self, prompt: str) -> Tuple[str, Dict[str, Any]]:
        """Parse natural-language prompt into intent category and tool parameters."""
        prompt_lower = prompt.lower().strip()

        # Check for explicit custom SELECT query
        if prompt_lower.startswith("select ") or prompt_lower.startswith("with "):
            return "tool_query_analytics", {"query_type": "custom", "custom_sql": prompt}

        # Data Warehouse OLAP intents
        if any(w in prompt_lower for w in ["warehouse", "star schema", "dim_", "fact_sales", "olap"]):
            if "customer" in prompt_lower:
                return "tool_query_warehouse", {"report_type": "top_customers"}
            elif "weekend" in prompt_lower or "weekday" in prompt_lower:
                return "tool_query_warehouse", {"report_type": "weekend_vs_weekday"}
            else:
                return "tool_query_warehouse", {"report_type": "category_revenue"}

        # Analytics / Revenue / Orders intents
        if any(w in prompt_lower for w in ["revenue", "kpi", "sales", "payment", "order"]):
            if "payment" in prompt_lower:
                return "tool_query_analytics", {"query_type": "payment_distribution"}
            elif "customer" in prompt_lower:
                return "tool_query_analytics", {"query_type": "customer_orders"}
            elif "reconcil" in prompt_lower:
                return "tool_query_analytics", {"query_type": "revenue_reconciliation"}
            elif "category" in prompt_lower and self.dw_path.exists():
                return "tool_query_warehouse", {"report_type": "category_revenue"}
            else:
                return "tool_query_analytics", {"query_type": "kpi"}

        # Data Quality intents
        if any(w in prompt_lower for w in ["quality", "audit", "null", "validator", "validation"]):
            return "tool_get_data_quality", {}

        # Lineage intents
        if any(w in prompt_lower for w in ["lineage", "dag", "dependency", "graph"]):
            return "tool_get_lineage", {}

        # Drift intents
        if any(w in prompt_lower for w in ["drift", "schema drift", "data drift", "variance"]):
            return "tool_get_drift_metrics", {}

        # Telemetry intents
        if any(w in prompt_lower for w in ["telemetry", "execution time", "runtime", "duration"]):
            return "tool_get_execution_telemetry", {}

        # CDC intents
        if any(w in prompt_lower for w in ["cdc", "incremental", "upsert", "delta"]):
            return "tool_get_cdc_summary", {}

        # Spark intents
        if any(w in prompt_lower for w in ["spark", "parquet", "lake", "pyspark"]):
            return "tool_get_spark_summary", {}

        # Default fallback to KPI analytics
        return "tool_query_analytics", {"query_type": "kpi"}

    # --- RESULT SYNTHESIZER ---

    def format_response(self, intent: str, params: Dict[str, Any], result: Dict[str, Any]) -> str:
        """Synthesize tool raw outputs into a formatted Markdown response with explanation."""
        if "error" in result:
            return f"[Error executing request]: {result['error']}"

        res_type = result.get("type", "")
        lines = []

        if res_type in ("kpi", "custom_sql", "payment_distribution", "customer_orders", "revenue_reconciliation"):
            rows = result.get("rows", [])
            lines.append(f"[SQL Analytics Result ({res_type.upper()})]:")
            if rows:
                keys = list(rows[0].keys())
                header = "| " + " | ".join(keys) + " |"
                divider = "| " + " | ".join(["---"] * len(keys)) + " |"
                lines.extend([header, divider])
                for r in rows[:10]:
                    row_str = "| " + " | ".join(str(r.get(k, "")) for k in keys) + " |"
                    lines.append(row_str)
                lines.append(f"\n*Explanation*: Retrieved {len(rows)} records safely via read-only SQLite engine.")
            else:
                lines.append("No records returned.")

        elif res_type.startswith("dw_"):
            rows = result.get("rows", [])
            lines.append(f"[Data Warehouse OLAP Report ({res_type.upper()})]:")
            if rows:
                keys = list(rows[0].keys())
                header = "| " + " | ".join(keys) + " |"
                divider = "| " + " | ".join(["---"] * len(keys)) + " |"
                lines.extend([header, divider])
                for r in rows[:10]:
                    row_str = "| " + " | ".join(str(r.get(k, "")) for k in keys) + " |"
                    lines.append(row_str)
                lines.append(f"\n*Explanation*: Star Schema Data Warehouse query executed against `ecommerce_dw.db`.")
            else:
                lines.append("No warehouse records found.")

        elif res_type == "data_quality":
            rep = result.get("report", {})
            summary = rep.get("summary", {})
            lines.append("[Data Quality Audit Summary]:")
            lines.append(f"- **Overall Passed**: {summary.get('overall_passed', False)}")
            lines.append(f"- **Total Checks**: {summary.get('total_checks', 0)}")
            lines.append(f"- **Passed Checks**: {summary.get('passed_checks', 0)}")
            lines.append(f"- **Failed Checks**: {summary.get('failed_checks', 0)}")
            lines.append("\n*Explanation*: Evaluated raw dataset schema completeness, null boundaries, and entity integrity.")

        elif res_type == "lineage":
            lin = result.get("lineage", {})
            nodes = lin.get("nodes", [])
            edges = lin.get("edges", [])
            lines.append("[Platform Lineage Summary]:")
            lines.append(f"- **Total Nodes**: {len(nodes)}")
            lines.append(f"- **Total Edges**: {len(edges)}")
            lines.append("Dependencies:")
            for e in edges:
                lines.append(f"  - `{e.get('source')}` --> `{e.get('target')}` ({e.get('transformation')})")
            lines.append("\n*Explanation*: Lineage graph maps data flow from raw CSV generation to SQLite & Parquet assets.")

        elif res_type == "drift":
            rep = result.get("drift_report", {})
            lines.append("[Platform Data Drift Analysis]:")
            lines.append(f"- **Data Drift Detected**: {rep.get('has_drift', False)}")
            lines.append(f"- **Threshold**: {rep.get('threshold', 0.20):.2%}")
            for metric, data in rep.get("metrics", {}).items():
                lines.append(f"  - `{metric}`: Baseline={data.get('baseline')}, Current={data.get('current')}, Variance={data.get('relative_change', 0):.2%}")
            lines.append("\n*Explanation*: Computed relative metric variance against established baselines.")

        elif res_type == "telemetry":
            telem = result.get("telemetry", {})
            lines.append("[Execution Telemetry Summary]:")
            tasks = telem.get("tasks", [])
            for t in tasks:
                lines.append(f"- Task `{t.get('task_id')}`: Status={t.get('status')}, Duration={t.get('duration_seconds')}s, Rows={t.get('row_count')}")
            lines.append("\n*Explanation*: Telemetry records task runtime performance and throughput.")

        elif res_type == "cdc":
            lines.append("[Incremental CDC Engine Status]:")
            lines.append(f"- **Status**: {result.get('status')}")
            lines.append(f"- **Primary Key**: `{result.get('primary_key')}`")
            lines.append(f"- **Supported Operations**: {', '.join(result.get('supported_operations', []))}")
            lines.append("\n*Explanation*: CDC engine manages key-based delta classification and idempotent UPSERT merges.")

        elif res_type == "spark":
            lines.append("[PySpark Data Lake Asset Summary]:")
            lines.append(f"- **Spark Processed Path**: `{result.get('output_directory')}`")
            lines.append(f"- **Category Revenue Parquet**: {result.get('category_revenue_parquet_exists')}")
            lines.append(f"- **Product Sales Summary Parquet**: {result.get('product_sales_summary_parquet_exists')}")
            lines.append("\n*Explanation*: PySpark batch engine writes schema-enforced partitioned Parquet data lake files.")

        else:
            lines.append(json.dumps(result, indent=2))

        return "\n".join(lines)

    # --- AGENT ENTRYPOINT ---

    def ask(self, prompt: str) -> str:
        """Process natural language request end-to-end."""
        tool_name, params = self.parse_intent(prompt)
        tool_fn = self.tools.get(tool_name)
        if not tool_fn:
            return f"[Error]: Unknown tool handler {tool_name}"

        try:
            result = tool_fn(**params)
            return self.format_response(tool_name, params, result)
        except Exception as e:
            return f"[Agent Processing Error]: {str(e)}"
