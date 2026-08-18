"""
Unit tests for Milestone 9 Platform Observability Engine (PlatformObservabilityEngine).
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

from src.observability import PlatformObservabilityEngine


def test_lineage_node_creation():
    """Test adding lineage nodes to graph."""
    engine = PlatformObservabilityEngine()
    engine.add_lineage_node("raw_orders", "raw_dataset", {"format": "csv"})
    engine.add_lineage_node("clean_orders", "processed_dataset", {"format": "json"})

    assert "raw_orders" in engine.nodes
    assert engine.nodes["raw_orders"]["type"] == "raw_dataset"
    assert engine.nodes["raw_orders"]["metadata"]["format"] == "csv"


def test_lineage_edge_creation():
    """Test adding directed lineage edges to graph."""
    engine = PlatformObservabilityEngine()
    engine.add_lineage_node("raw_orders", "raw_dataset")
    engine.add_lineage_node("clean_orders", "processed_dataset")
    engine.add_lineage_edge("raw_orders", "clean_orders", "TRANSFORMS_TO")

    assert len(engine.edges) == 1
    assert engine.edges[0]["source"] == "raw_orders"
    assert engine.edges[0]["target"] == "clean_orders"
    assert engine.edges[0]["relationship"] == "TRANSFORMS_TO"


def test_telemetry_recording():
    """Test recording task execution metrics and telemetry log serialization."""
    engine = PlatformObservabilityEngine()
    rec = engine.record_execution_telemetry("etl_pipeline", duration_seconds=1.234, row_count=1000, status="SUCCESS")

    assert rec["task_name"] == "etl_pipeline"
    assert rec["duration_seconds"] == 1.234
    assert rec["row_count"] == 1000
    assert rec["status"] == "SUCCESS"
    assert len(engine.telemetry_logs) == 1


def test_schema_drift_detection():
    """Test detecting missing columns, added columns, and type mismatches."""
    engine = PlatformObservabilityEngine()
    expected = {"order_id": "str", "total_price": "float", "status": "str"}
    actual = {"order_id": "str", "total_price": "int", "discount": "float"}

    res = engine.detect_schema_drift(expected, actual)
    assert res["has_drift"] is True
    assert "status" in res["missing_columns"]
    assert "discount" in res["added_columns"]
    assert len(res["type_mismatches"]) == 1
    assert res["type_mismatches"][0]["column"] == "total_price"


def test_data_drift_detection():
    """Test detecting numerical metric drift beyond baseline threshold."""
    engine = PlatformObservabilityEngine(threshold=0.20)  # 20% threshold

    # 10% shift -> No drift
    no_drift_res = engine.detect_data_drift(baseline_metric=100.0, current_metric=110.0)
    assert no_drift_res["has_drift"] is False
    assert no_drift_res["relative_change"] == 0.10

    # 30% shift -> Drift detected
    drift_res = engine.detect_data_drift(baseline_metric=100.0, current_metric=130.0)
    assert drift_res["has_drift"] is True
    assert drift_res["relative_change"] == 0.30


def test_lineage_and_telemetry_json_export(tmp_path):
    """Test serializing lineage and telemetry to JSON files."""
    engine = PlatformObservabilityEngine()
    engine.add_lineage_node("raw_orders", "raw_dataset")
    engine.add_lineage_node("db_orders", "database_table")
    engine.add_lineage_edge("raw_orders", "db_orders", "LOADS_INTO")
    engine.record_execution_telemetry("load_db", duration_seconds=0.5, row_count=500)

    lineage_file = str(tmp_path / "lineage.json")
    telemetry_file = str(tmp_path / "telemetry.json")

    engine.export_lineage_json(lineage_file)
    engine.export_telemetry_json(telemetry_file)

    assert os.path.exists(lineage_file)
    assert os.path.exists(telemetry_file)

    with open(lineage_file, "r") as f:
        lineage_data = json.load(f)
        assert lineage_data["total_nodes"] == 2
        assert lineage_data["total_edges"] == 1

    with open(telemetry_file, "r") as f:
        telemetry_data = json.load(f)
        assert telemetry_data["total_records"] == 1
