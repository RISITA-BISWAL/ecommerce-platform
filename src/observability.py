"""
Platform Lineage and Observability Engine for E-Commerce Data Platform.

Provides capability to:
1. Track dataset lineage nodes and transformation edges across the pipeline.
2. Record execution telemetry (execution time, status, row counts).
3. Detect schema drift (added, removed, or modified columns/data types).
4. Detect numerical data drift using a deterministic relative change threshold.
5. Serialize lineage and observability telemetry metadata to JSON.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional, Set


class PlatformObservabilityEngine:
    """Manages platform lineage graph, execution telemetry metrics, and drift detection."""

    def __init__(self, threshold: float = 0.20):
        """Initialize engine with configurable data drift threshold (default 20%)."""
        self.threshold = threshold
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: List[Dict[str, str]] = []
        self.telemetry_logs: List[Dict[str, Any]] = []

    def add_lineage_node(self, node_id: str, node_type: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Add a dataset or transformation node to the lineage graph."""
        self.nodes[node_id] = {
            "node_id": node_id,
            "type": node_type,  # e.g., 'raw_dataset', 'transformer', 'database', 'parquet'
            "metadata": metadata or {},
        }

    def add_lineage_edge(self, source_id: str, target_id: str, relationship: str = "TRANSFORMS_TO") -> None:
        """Add a directed edge connecting source dataset/task to target dataset/task."""
        edge = {"source": source_id, "target": target_id, "relationship": relationship}
        if edge not in self.edges:
            self.edges.append(edge)

    def record_execution_telemetry(
        self,
        task_name: str,
        duration_seconds: float,
        row_count: int,
        status: str = "SUCCESS",
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Record task execution duration, row counts, and status telemetry."""
        record = {
            "task_name": task_name,
            "duration_seconds": round(duration_seconds, 4),
            "row_count": row_count,
            "status": status,
            "timestamp": time.time(),
            "details": details or {},
        }
        self.telemetry_logs.append(record)
        return record

    def detect_schema_drift(
        self,
        expected_schema: Dict[str, str],
        actual_schema: Dict[str, str],
    ) -> Dict[str, Any]:
        """Compare expected vs actual column schemas to detect schema drift.

        Returns:
            Dict containing 'has_drift', 'missing_columns', 'added_columns', 'type_mismatches'.
        """
        expected_cols: Set[str] = set(expected_schema.keys())
        actual_cols: Set[str] = set(actual_schema.keys())

        missing = sorted(list(expected_cols - actual_cols))
        added = sorted(list(actual_cols - expected_cols))

        type_mismatches = []
        for col in expected_cols.intersection(actual_cols):
            if expected_schema[col] != actual_schema[col]:
                type_mismatches.append(
                    {
                        "column": col,
                        "expected": expected_schema[col],
                        "actual": actual_schema[col],
                    }
                )

        has_drift = len(missing) > 0 or len(added) > 0 or len(type_mismatches) > 0

        return {
            "has_drift": has_drift,
            "missing_columns": missing,
            "added_columns": added,
            "type_mismatches": type_mismatches,
        }

    def detect_data_drift(
        self,
        baseline_metric: float,
        current_metric: float,
    ) -> Dict[str, Any]:
        """Detect numeric data drift based on relative deviation from baseline metric.

        Relative deviation formula: |current - baseline| / max(|baseline|, 1e-9)

        Returns:
            Dict containing 'has_drift', 'relative_change', 'threshold', 'baseline', 'current'.
        """
        if baseline_metric == 0:
            rel_change = abs(current_metric)
        else:
            rel_change = abs(current_metric - baseline_metric) / abs(baseline_metric)

        has_drift = rel_change > self.threshold

        return {
            "has_drift": has_drift,
            "relative_change": round(rel_change, 4),
            "threshold": self.threshold,
            "baseline": baseline_metric,
            "current": current_metric,
        }

    def export_lineage_json(self, output_path: str) -> str:
        """Serialize lineage nodes and edges to JSON file."""
        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        data = {
            "nodes": list(self.nodes.values()),
            "edges": self.edges,
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return output_path

    def export_telemetry_json(self, output_path: str) -> str:
        """Serialize execution telemetry metrics to JSON file."""
        parent_dir = os.path.dirname(output_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        data = {
            "telemetry_records": self.telemetry_logs,
            "total_records": len(self.telemetry_logs),
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return output_path
