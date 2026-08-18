"""
Data Warehouse & Star Schema Modeling Engine for E-Commerce Data Platform.

Provides capability to:
1. Initialize Star Schema tables (dim_customer, dim_product, dim_date, fact_sales).
2. Populate pre-computed Date dimension for time-series analytics.
3. Populate Customer and Product conformed dimensions from OLTP source data.
4. Load central Fact Sales table with surrogate key lookups.
5. Execute multi-dimensional OLAP analytics queries (drill-downs, roll-ups).
"""

import datetime
import sqlite3
from typing import Optional, Tuple
import pandas as pd


class EcommerceDataWarehouse:
    """Manages Star Schema data warehouse creation, ELT population, and OLAP querying."""

    def __init__(
        self,
        db_path: str = "data/ecommerce.db",
        dw_path: str = "data/ecommerce_dw.db",
    ):
        """Initialize data warehouse manager with OLTP source path and DW target path."""
        self.db_path = db_path
        self.dw_path = dw_path

    def get_dw_connection(self) -> sqlite3.Connection:
        """Connect to SQLite data warehouse database."""
        return sqlite3.connect(self.dw_path)

    def get_oltp_connection(self) -> sqlite3.Connection:
        """Connect to SQLite OLTP database."""
        return sqlite3.connect(self.db_path)

    def create_warehouse_schema(self) -> None:
        """Create Star Schema dimension and fact tables with primary, foreign, and surrogate keys."""
        conn = self.get_dw_connection()
        cursor = conn.cursor()

        # 1. Customer Dimension
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                email TEXT,
                signup_channel TEXT,
                created_at TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        )

        # 2. Product Dimension
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        )

        # 3. Date Dimension
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key INTEGER PRIMARY KEY,
                full_date TEXT NOT NULL UNIQUE,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                month INTEGER NOT NULL,
                month_name TEXT NOT NULL,
                day_of_month INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                day_name TEXT NOT NULL,
                is_weekend INTEGER NOT NULL
            );
        """
        )

        # 4. Sales Fact Table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_sales (
                sales_fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL UNIQUE,
                customer_key INTEGER NOT NULL,
                product_key INTEGER NOT NULL,
                date_key INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                total_price REAL NOT NULL,
                order_status TEXT NOT NULL,
                payment_method TEXT,
                FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
            );
        """
        )

        # Indexes for fast surrogate key lookups
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_dim_cust_id ON dim_customer(customer_id);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_dim_prod_id ON dim_product(product_id);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_fact_cust_key ON fact_sales(customer_key);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_fact_prod_key ON fact_sales(product_key);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_fact_date_key ON fact_sales(date_key);"
        )

        conn.commit()
        conn.close()

    def populate_dim_date(
        self, start_year: int = 2024, end_year: int = 2026
    ) -> int:
        """Populates time dimension table with continuous calendar dates and extracted attributes."""
        conn = self.get_dw_connection()
        cursor = conn.cursor()

        start_date = datetime.date(start_year, 1, 1)
        end_date = datetime.date(end_year, 12, 31)
        current_date = start_date

        inserted_count = 0
        while current_date <= end_date:
            date_key = int(current_date.strftime("%Y%m%d"))
            full_date = current_date.strftime("%Y-%m-%d")
            year = current_date.year
            quarter = (current_date.month - 1) // 3 + 1
            month = current_date.month
            month_name = current_date.strftime("%B")
            day_of_month = current_date.day
            day_of_week = current_date.weekday()  # 0=Monday, 6=Sunday
            day_name = current_date.strftime("%A")
            is_weekend = 1 if day_of_week >= 5 else 0

            cursor.execute(
                """
                INSERT OR IGNORE INTO dim_date (
                    date_key, full_date, year, quarter, month, month_name,
                    day_of_month, day_of_week, day_name, is_weekend
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
                (
                    date_key,
                    full_date,
                    year,
                    quarter,
                    month,
                    month_name,
                    day_of_month,
                    day_of_week,
                    day_name,
                    is_weekend,
                ),
            )
            if cursor.rowcount > 0:
                inserted_count += 1

            current_date += datetime.timedelta(days=1)

        conn.commit()
        conn.close()
        return inserted_count

    def populate_dim_customer(
        self, oltp_conn: Optional[sqlite3.Connection] = None
    ) -> int:
        """Extracts customer records from OLTP database and loads dim_customer."""
        close_oltp = False
        if oltp_conn is None:
            oltp_conn = self.get_oltp_connection()
            close_oltp = True

        df_cust = pd.read_sql_query(
            "SELECT customer_id, name, email, signup_channel, created_at FROM customers",
            oltp_conn,
        )

        if close_oltp:
            oltp_conn.close()

        dw_conn = self.get_dw_connection()
        cursor = dw_conn.cursor()

        count = 0
        for _, row in df_cust.iterrows():
            cursor.execute(
                """
                INSERT INTO dim_customer (customer_id, name, email, signup_channel, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(customer_id) DO UPDATE SET
                    name=excluded.name,
                    email=excluded.email,
                    signup_channel=excluded.signup_channel,
                    created_at=excluded.created_at,
                    updated_at=CURRENT_TIMESTAMP;
            """,
                (
                    row["customer_id"],
                    row["name"],
                    row["email"],
                    row["signup_channel"],
                    row["created_at"],
                ),
            )
            count += 1

        dw_conn.commit()
        dw_conn.close()
        return count

    def populate_dim_product(
        self, oltp_conn: Optional[sqlite3.Connection] = None
    ) -> int:
        """Extracts product records from OLTP database and loads dim_product."""
        close_oltp = False
        if oltp_conn is None:
            oltp_conn = self.get_oltp_connection()
            close_oltp = True

        df_prod = pd.read_sql_query(
            "SELECT product_id, name, category, price FROM products", oltp_conn
        )

        if close_oltp:
            oltp_conn.close()

        dw_conn = self.get_dw_connection()
        cursor = dw_conn.cursor()

        count = 0
        for _, row in df_prod.iterrows():
            cursor.execute(
                """
                INSERT INTO dim_product (product_id, name, category, price)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    name=excluded.name,
                    category=excluded.category,
                    price=excluded.price,
                    updated_at=CURRENT_TIMESTAMP;
            """,
                (row["product_id"], row["name"], row["category"], float(row["price"])),
            )
            count += 1

        dw_conn.commit()
        dw_conn.close()
        return count

    def populate_fact_sales(
        self, oltp_conn: Optional[sqlite3.Connection] = None
    ) -> int:
        """Joins OLTP orders & payments with DW dimensions to populate fact_sales table."""
        close_oltp = False
        if oltp_conn is None:
            oltp_conn = self.get_oltp_connection()
            close_oltp = True

        query_oltp = """
            SELECT 
                o.order_id,
                o.customer_id,
                o.product_id,
                o.order_date,
                o.quantity,
                ROUND(o.total_price / o.quantity, 2) AS unit_price,
                o.total_price,
                o.status AS order_status,
                p.payment_method
            FROM orders o
            LEFT JOIN payments p ON o.order_id = p.order_id
        """
        df_orders = pd.read_sql_query(query_oltp, oltp_conn)

        if close_oltp:
            oltp_conn.close()

        dw_conn = self.get_dw_connection()
        
        # Load dimension mapping dicts for surrogate key resolution
        df_dim_cust = pd.read_sql_query("SELECT customer_key, customer_id FROM dim_customer", dw_conn)
        df_dim_prod = pd.read_sql_query("SELECT product_key, product_id FROM dim_product", dw_conn)
        df_dim_date = pd.read_sql_query("SELECT date_key, full_date FROM dim_date", dw_conn)

        cust_map = dict(zip(df_dim_cust["customer_id"], df_dim_cust["customer_key"]))
        prod_map = dict(zip(df_dim_prod["product_id"], df_dim_prod["product_key"]))
        date_map = dict(zip(df_dim_date["full_date"], df_dim_date["date_key"]))

        cursor = dw_conn.cursor()
        inserted_count = 0

        for _, row in df_orders.iterrows():
            cust_key = cust_map.get(row["customer_id"])
            prod_key = prod_map.get(row["product_id"])
            
            # Format order_date (YYYY-MM-DD) to date_key integer
            order_date_str = str(row["order_date"]).split(" ")[0]
            date_key = date_map.get(order_date_str)

            if not cust_key or not prod_key or not date_key:
                continue

            cursor.execute(
                """
                INSERT INTO fact_sales (
                    order_id, customer_key, product_key, date_key,
                    quantity, unit_price, total_price, order_status, payment_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    customer_key=excluded.customer_key,
                    product_key=excluded.product_key,
                    date_key=excluded.date_key,
                    quantity=excluded.quantity,
                    unit_price=excluded.unit_price,
                    total_price=excluded.total_price,
                    order_status=excluded.order_status,
                    payment_method=excluded.payment_method;
            """,
                (
                    row["order_id"],
                    cust_key,
                    prod_key,
                    date_key,
                    int(row["quantity"]),
                    float(row["unit_price"]),
                    float(row["total_price"]),
                    row["order_status"],
                    row["payment_method"] if pd.notnull(row["payment_method"]) else "Unknown",
                ),
            )
            inserted_count += 1

        dw_conn.commit()
        dw_conn.close()
        return inserted_count

    def build_full_warehouse(self) -> dict:
        """Executes full Data Warehouse ELT build sequence."""
        self.create_warehouse_schema()
        dates_count = self.populate_dim_date()
        cust_count = self.populate_dim_customer()
        prod_count = self.populate_dim_product()
        facts_count = self.populate_fact_sales()

        return {
            "dim_date_records": dates_count,
            "dim_customer_records": cust_count,
            "dim_product_records": prod_count,
            "fact_sales_records": facts_count,
            "status": "SUCCESS",
        }

    # --- OLAP ANALYTICS QUERIES ---
    def get_category_revenue_drilldown(self) -> pd.DataFrame:
        """OLAP aggregation drill-down: Category revenue by year and quarter."""
        dw_conn = self.get_dw_connection()
        query = """
            SELECT 
                d.year,
                d.quarter,
                p.category,
                COUNT(DISTINCT f.order_id) AS total_orders,
                SUM(f.quantity) AS total_items_sold,
                ROUND(SUM(f.total_price), 2) AS category_revenue
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.year, d.quarter, p.category
            ORDER BY d.year, d.quarter, category_revenue DESC;
        """
        df = pd.read_sql_query(query, dw_conn)
        dw_conn.close()
        return df

    def get_weekend_sales_performance(self) -> pd.DataFrame:
        """OLAP comparison: Sales metrics on weekends vs weekdays."""
        dw_conn = self.get_dw_connection()
        query = """
            SELECT 
                CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
                COUNT(DISTINCT f.order_id) AS total_orders,
                SUM(f.quantity) AS total_items_sold,
                ROUND(SUM(f.total_price), 2) AS total_revenue,
                ROUND(AVG(f.total_price), 2) AS avg_order_value
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.is_weekend;
        """
        df = pd.read_sql_query(query, dw_conn)
        dw_conn.close()
        return df
