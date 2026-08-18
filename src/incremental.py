"""
Incremental Change Data Capture (CDC) Processor for E-Commerce Data Platform.

Simulates CDC delta batch processing by:
1. Comparing incoming delta records against an existing baseline dataset/database table.
2. Classifying records into INSERT, UPDATE, or NO_CHANGE based on a primary key.
3. Tracking specific changed columns for UPDATE records.
4. Performing idempotent merge/upsert operations into SQLite tables or list datasets.
"""

import sqlite3
from typing import Any, Dict, List, Tuple


class IncrementalCDCProcessor:
    """Processes batch deltas and performs CDC classification and idempotent upserts."""

    def __init__(self, primary_key: str = "order_id"):
        """Initialize CDC processor with a target primary key column."""
        self.primary_key = primary_key

    def classify_delta(
        self,
        current_data: List[Dict[str, Any]],
        incoming_delta: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Classify incoming delta records against current data.

        Args:
            current_data: List of existing records.
            incoming_delta: List of incoming delta records.

        Returns:
            Dictionary containing CDC classification summary and categorized records.
        """
        current_lookup = {
            str(row[self.primary_key]): row for row in current_data if self.primary_key in row
        }

        inserts: List[Dict[str, Any]] = []
        updates: List[Dict[str, Any]] = []
        no_changes: List[Dict[str, Any]] = []

        for delta_row in incoming_delta:
            pk_val = str(delta_row.get(self.primary_key, ""))
            if not pk_val or pk_val not in current_lookup:
                # New record -> INSERT
                row_copy = dict(delta_row)
                row_copy["_cdc_action"] = "INSERT"
                row_copy["_changed_columns"] = []
                inserts.append(row_copy)
            else:
                existing_row = current_lookup[pk_val]
                changed_cols = []
                for k, v in delta_row.items():
                    if k in existing_row and str(existing_row[k]) != str(v):
                        changed_cols.append(k)

                row_copy = dict(delta_row)
                if changed_cols:
                    # Existing record with differences -> UPDATE
                    row_copy["_cdc_action"] = "UPDATE"
                    row_copy["_changed_columns"] = changed_cols
                    updates.append(row_copy)
                else:
                    # Identical record -> NO_CHANGE
                    row_copy["_cdc_action"] = "NO_CHANGE"
                    row_copy["_changed_columns"] = []
                    no_changes.append(row_copy)

        classified_records = inserts + updates + no_changes
        summary = {
            "INSERT": len(inserts),
            "UPDATE": len(updates),
            "NO_CHANGE": len(no_changes),
            "total_delta_records": len(incoming_delta),
        }

        return {
            "summary": summary,
            "inserts": inserts,
            "updates": updates,
            "no_changes": no_changes,
            "classified_records": classified_records,
        }

    def merge_into_dataset(
        self,
        current_data: List[Dict[str, Any]],
        incoming_delta: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Merge incoming delta records into current dataset idempotently.

        Args:
            current_data: List of existing records.
            incoming_delta: List of incoming delta records.

        Returns:
            Tuple of (merged_dataset, cdc_summary)
        """
        classification = self.classify_delta(current_data, incoming_delta)
        merged_dict = {
            str(row[self.primary_key]): dict(row)
            for row in current_data
            if self.primary_key in row
        }

        # Apply inserts and updates (excluding metadata tags in merged dataset)
        for row in classification["inserts"] + classification["updates"]:
            pk_val = str(row[self.primary_key])
            clean_row = {k: v for k, v in row.items() if not k.startswith("_cdc_")}
            merged_dict[pk_val] = clean_row

        merged_list = list(merged_dict.values())
        return merged_list, classification["summary"]

    def upsert_sqlite_table(
        self,
        db_path: str,
        table_name: str,
        incoming_delta: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Perform idempotent SQLite UPSERT for incoming delta records into a database table.

        Args:
            db_path: Path to SQLite database file.
            table_name: Target table name.
            incoming_delta: List of incoming delta records.

        Returns:
            CDC summary dictionary.
        """
        if not incoming_delta:
            return {"INSERT": 0, "UPDATE": 0, "NO_CHANGE": 0, "total_delta_records": 0}

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Fetch existing table rows
        cursor.execute(f"SELECT * FROM {table_name}")
        existing_rows = [dict(row) for row in cursor.fetchall()]

        classification = self.classify_delta(existing_rows, incoming_delta)
        summary = classification["summary"]

        to_upsert = classification["inserts"] + classification["updates"]
        if not to_upsert:
            conn.close()
            return summary

        cols = [k for k in incoming_delta[0].keys() if not k.startswith("_cdc_")]
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        update_set = ", ".join([f"{col} = excluded.{col}" for col in cols if col != self.primary_key])

        sql = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT({self.primary_key}) DO UPDATE SET
        {update_set}
        """

        for row in to_upsert:
            vals = [row.get(col) for col in cols]
            cursor.execute(sql, vals)

        conn.commit()
        conn.close()

        return summary
