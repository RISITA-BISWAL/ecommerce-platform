import sqlite3
from typing import Optional
import pandas as pd

class EcommerceAnalytics:
    """
    Read-only SQL Analytics engine for E-Commerce data engineering.
    Executes structured SQL queries against SQLite database and returns Pandas DataFrames.
    """
    def __init__(self, db_path: str = "data/ecommerce.db"):
        self.db_path = db_path

    def get_connection(self) -> sqlite3.Connection:
        """Opens a read-only SQLite database connection."""
        # Using URI read-only mode guarantees queries cannot mutate the database
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        except sqlite3.OperationalError:
            # Fallback to standard connection if URI mode is unsupported
            conn = sqlite3.connect(self.db_path)
        return conn

    def _execute_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """Executes a SELECT query safely and returns a Pandas DataFrame."""
        conn = self.get_connection()
        try:
            df = pd.read_sql_query(query, conn, params=params)
        finally:
            conn.close()
        return df

    def get_executive_kpis(self) -> pd.DataFrame:
        """Computes executive-level Key Performance Indicators (KPIs)."""
        query = """
            SELECT 
                (SELECT COUNT(*) FROM customers) AS total_customers,
                (SELECT COUNT(*) FROM products) AS total_products,
                (SELECT COUNT(*) FROM orders) AS total_orders,
                COALESCE((SELECT ROUND(SUM(total_price), 2) FROM orders), 0.0) AS total_revenue,
                COALESCE((SELECT ROUND(AVG(total_price), 2) FROM orders), 0.0) AS average_order_value,
                COALESCE((SELECT SUM(quantity) FROM orders), 0) AS total_quantity_sold,
                COALESCE((SELECT COUNT(*) FROM payments WHERE status = 'Completed'), 0) AS successful_payments,
                COALESCE((SELECT ROUND(AVG(rating), 2) FROM reviews), 0.0) AS average_product_rating;
        """
        return self._execute_query(query)

    def reconcile_revenue(self) -> pd.DataFrame:
        """
        Reconciles order revenue by comparing orders.total_price with calculated (quantity * unit_price).
        Reports any revenue discrepancies.
        """
        query = """
            SELECT 
                o.order_id,
                o.customer_id,
                o.product_id,
                o.quantity,
                p.price AS unit_price,
                o.total_price AS order_total_price,
                ROUND(o.quantity * p.price, 2) AS calculated_revenue,
                ROUND(o.total_price - (o.quantity * p.price), 2) AS discrepancy_amount
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE ROUND(o.total_price, 2) != ROUND(o.quantity * p.price, 2);
        """
        return self._execute_query(query)

    # --- 1. SALES ANALYTICS ---
    def get_overall_sales_summary(self) -> pd.DataFrame:
        """Summarizes overall sales performance."""
        query = """
            SELECT 
                COUNT(order_id) AS total_orders,
                COALESCE(SUM(quantity), 0) AS total_units_sold,
                COALESCE(ROUND(SUM(total_price), 2), 0.0) AS total_revenue,
                COALESCE(ROUND(AVG(total_price), 2), 0.0) AS avg_order_value,
                COALESCE(ROUND(MIN(total_price), 2), 0.0) AS min_order_value,
                COALESCE(ROUND(MAX(total_price), 2), 0.0) AS max_order_value
            FROM orders;
        """
        return self._execute_query(query)

    def get_orders_by_status(self) -> pd.DataFrame:
        """Calculates order count and total revenue grouped by order status."""
        query = """
            SELECT 
                status AS order_status,
                COUNT(order_id) AS order_count,
                COALESCE(ROUND(SUM(total_price), 2), 0.0) AS total_revenue
            FROM orders
            GROUP BY status
            ORDER BY order_count DESC;
        """
        return self._execute_query(query)

    # --- 2. PRODUCT ANALYTICS ---
    def get_top_selling_products(self, limit: int = 10) -> pd.DataFrame:
        """Returns top-selling products by revenue and quantity."""
        query = """
            SELECT 
                p.product_id,
                p.name AS product_name,
                p.category,
                p.price AS unit_price,
                COALESCE(SUM(o.quantity), 0) AS total_quantity_sold,
                COALESCE(ROUND(SUM(o.total_price), 2), 0.0) AS total_revenue
            FROM products p
            LEFT JOIN orders o ON p.product_id = o.product_id
            GROUP BY p.product_id, p.name, p.category, p.price
            ORDER BY total_revenue DESC
            LIMIT ?;
        """
        return self._execute_query(query, (limit,))

    def get_revenue_by_category(self) -> pd.DataFrame:
        """Aggregates revenue and quantity sold grouped by product category."""
        query = """
            SELECT 
                p.category,
                COUNT(DISTINCT p.product_id) AS product_count,
                COALESCE(SUM(o.quantity), 0) AS total_quantity_sold,
                COALESCE(ROUND(SUM(o.total_price), 2), 0.0) AS total_revenue
            FROM products p
            LEFT JOIN orders o ON p.product_id = o.product_id
            GROUP BY p.category
            ORDER BY total_revenue DESC;
        """
        return self._execute_query(query)

    # --- 3. CUSTOMER ANALYTICS ---
    def get_customer_analytics(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Summarizes customer ordering patterns using LEFT JOIN so customers with 0 orders are included.
        """
        query = """
            SELECT 
                c.customer_id,
                c.name AS customer_name,
                c.email,
                c.signup_channel,
                COUNT(o.order_id) AS total_orders,
                COALESCE(SUM(o.quantity), 0) AS total_items_purchased,
                COALESCE(ROUND(SUM(o.total_price), 2), 0.0) AS total_spend,
                COALESCE(ROUND(AVG(o.total_price), 2), 0.0) AS avg_order_spend
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.name, c.email, c.signup_channel
            ORDER BY total_spend DESC;
        """
        df = self._execute_query(query)
        if limit and isinstance(limit, int):
            df = df.head(limit)
        return df

    # --- 4. PAYMENT ANALYTICS ---
    def get_payment_method_distribution(self) -> pd.DataFrame:
        """Aggregates payment volume and amounts by payment method."""
        query = """
            SELECT 
                payment_method,
                COUNT(payment_id) AS payment_count,
                COALESCE(ROUND(SUM(amount), 2), 0.0) AS total_amount
            FROM payments
            GROUP BY payment_method
            ORDER BY payment_count DESC;
        """
        return self._execute_query(query)

    def get_payment_status_breakdown(self) -> pd.DataFrame:
        """Aggregates payments by payment status (Completed, Pending, Failed, Refunded)."""
        query = """
            SELECT 
                status AS payment_status,
                COUNT(payment_id) AS payment_count,
                COALESCE(ROUND(SUM(amount), 2), 0.0) AS total_amount
            FROM payments
            GROUP BY status
            ORDER BY payment_count DESC;
        """
        return self._execute_query(query)

    # --- 5. REVIEW ANALYTICS ---
    def get_product_review_ratings(self, limit: int = 10) -> pd.DataFrame:
        """Calculates average rating and review counts per product."""
        query = """
            SELECT 
                p.product_id,
                p.name AS product_name,
                p.category,
                COUNT(r.review_id) AS review_count,
                COALESCE(ROUND(AVG(r.rating), 2), 0.0) AS average_rating
            FROM products p
            LEFT JOIN reviews r ON p.product_id = r.product_id
            GROUP BY p.product_id, p.name, p.category
            ORDER BY average_rating DESC, review_count DESC
            LIMIT ?;
        """
        return self._execute_query(query, (limit,))

    def get_rating_category_distribution(self) -> pd.DataFrame:
        """Groups customer reviews by sentiment category using a SQL CASE expression."""
        query = """
            SELECT 
                CASE 
                    WHEN rating >= 4 THEN 'Positive (4-5 Stars)'
                    WHEN rating = 3 THEN 'Neutral (3 Stars)'
                    ELSE 'Negative (1-2 Stars)'
                END AS sentiment_category,
                COUNT(review_id) AS review_count,
                ROUND(COUNT(review_id) * 100.0 / (SELECT COUNT(*) FROM reviews), 2) AS percentage
            FROM reviews
            GROUP BY sentiment_category
            ORDER BY review_count DESC;
        """
        return self._execute_query(query)

    # --- 6. TIME-BASED ANALYTICS ---
    def get_monthly_trends(self) -> pd.DataFrame:
        """Aggregates orders and revenue trends grouped by month."""

        query = """
            SELECT 
                strftime('%Y-%m', order_date) AS order_month,
                COUNT(order_id) AS total_orders,
                COALESCE(SUM(quantity), 0) AS total_units_sold,
                COALESCE(ROUND(SUM(total_price), 2), 0.0) AS monthly_revenue,
                COALESCE(ROUND(AVG(total_price), 2), 0.0) AS avg_order_value
            FROM orders
            GROUP BY order_month
            ORDER BY order_month ASC;
        """
        return self._execute_query(query)
