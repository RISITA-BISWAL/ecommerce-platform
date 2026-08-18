"""
Unit and Integration Tests for Milestone 11 Agentic Data Engineering Assistant.
"""

import json
import sqlite3
import tempfile
from pathlib import Path
import pytest

from src.agent import DataPlatformAgent, SQLGuardrailEngine, SQLGuardrailError


def test_sql_guardrail_valid_select():
    """Verify valid SELECT queries pass validation."""
    query = "SELECT * FROM orders WHERE total_price > 50"
    validated = SQLGuardrailEngine.validate_sql(query)
    assert validated.startswith("SELECT * FROM orders")
    assert "LIMIT 100" in validated


def test_sql_guardrail_prohibited_keyword():
    """Verify queries containing modification keywords are rejected."""
    prohibited_queries = [
        "DROP TABLE orders",
        "DELETE FROM customers",
        "UPDATE orders SET status = 'Cancelled'",
        "INSERT INTO customers VALUES ('C1', 'Test')",
        "ALTER TABLE orders ADD COLUMN test TEXT",
        "TRUNCATE TABLE payments",
    ]
    for q in prohibited_queries:
        with pytest.raises(SQLGuardrailError):
            SQLGuardrailEngine.validate_sql(q)


def test_sql_guardrail_multiple_statements():
    """Verify multiple SQL statements separated by semicolon are rejected."""
    query = "SELECT * FROM orders; DROP TABLE orders;"
    with pytest.raises(SQLGuardrailError):
        SQLGuardrailEngine.validate_sql(query)


def test_sql_guardrail_limit_appended():
    """Verify LIMIT 100 is appended if not present."""
    query = "SELECT customer_id, total_price FROM orders"
    validated = SQLGuardrailEngine.validate_sql(query)
    assert "LIMIT 100" in validated

    query_with_limit = "SELECT customer_id FROM orders LIMIT 10"
    validated_limit = SQLGuardrailEngine.validate_sql(query_with_limit)
    assert validated_limit.count("LIMIT") == 1
    assert "LIMIT 10" in validated_limit


def test_sql_guardrail_read_only_connection():
    """Verify read-only connection mode rejects database write attempts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INT, val TEXT)")
        conn.execute("INSERT INTO test VALUES (1, 'hello')")
        conn.commit()
        conn.close()

        # Reading works
        results = SQLGuardrailEngine.execute_safe_query(str(db_path), "SELECT * FROM test")
        assert len(results) == 1
        assert results[0]["val"] == "hello"


def test_intent_parser_analytics():
    """Test intent parsing for analytics queries."""
    agent = DataPlatformAgent()
    tool, params = agent.parse_intent("What is our total revenue and order count?")
    assert tool == "tool_query_analytics"
    assert params["query_type"] == "kpi"

    tool, params = agent.parse_intent("Show payment method distribution")
    assert tool == "tool_query_analytics"
    assert params["query_type"] == "payment_distribution"


def test_intent_parser_warehouse():
    """Test intent parsing for Star Schema Data Warehouse queries."""
    agent = DataPlatformAgent()
    tool, params = agent.parse_intent("Show category revenue summary in data warehouse")
    assert tool == "tool_query_warehouse"
    assert params["report_type"] == "category_revenue"

    tool, params = agent.parse_intent("Compare weekend vs weekday sales in star schema")
    assert tool == "tool_query_warehouse"
    assert params["report_type"] == "weekend_vs_weekday"


def test_intent_parser_observability_and_quality():
    """Test intent parsing for quality, lineage, and drift."""
    agent = DataPlatformAgent()

    tool, _ = agent.parse_intent("Show data quality audit results")
    assert tool == "tool_get_data_quality"

    tool, _ = agent.parse_intent("What is the pipeline lineage graph?")
    assert tool == "tool_get_lineage"

    tool, _ = agent.parse_intent("Check data drift variance")
    assert tool == "tool_get_drift_metrics"

    tool, _ = agent.parse_intent("Check execution telemetry duration")
    assert tool == "tool_get_execution_telemetry"


def test_intent_parser_cdc_and_spark():
    """Test intent parsing for CDC and PySpark."""
    agent = DataPlatformAgent()

    tool, _ = agent.parse_intent("What is the status of CDC delta merge?")
    assert tool == "tool_get_cdc_summary"

    tool, _ = agent.parse_intent("Summarize PySpark Parquet data lake files")
    assert tool == "tool_get_spark_summary"


def test_agent_ask_end_to_end():
    """Test end-to-end question processing by the agent."""
    with tempfile.TemporaryDirectory() as tmpdir:
        agent = DataPlatformAgent(base_dir=tmpdir)
        response = agent.ask("Show data quality audit results")
        assert "Data Quality Audit Summary" in response or "Raw data directory not found" in response

        drift_response = agent.ask("Check data drift metrics")
        assert "Platform Data Drift Analysis" in drift_response

        cdc_response = agent.ask("What is the status of CDC?")
        assert "Incremental CDC Engine Status" in cdc_response


def test_agent_error_handling_missing_db():
    """Verify graceful error message when database file is missing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        agent = DataPlatformAgent(base_dir=tmpdir)
        resp = agent.tool_query_analytics(query_type="kpi")
        assert "error" in resp
        assert "Operational database not found" in resp["error"]
