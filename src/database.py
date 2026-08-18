import csv
import os
import sqlite3

class EcommerceDatabase:
    """
    Manages SQLite database connection, schema creation,
    explicit UPSERT CSV data ingestion, and relational integrity validation.
    """
    def __init__(self, db_path: str = os.path.join("data", "ecommerce.db")):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def get_connection(self) -> sqlite3.Connection:
        """Creates and returns a SQLite database connection with foreign key enforcement ON."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def create_tables(self) -> None:
        """Create the 5 relational tables if they do not already exist."""
        conn = self.get_connection()
        cursor = conn.cursor()

        # 1. Customers Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TEXT NOT NULL,
                signup_channel TEXT NOT NULL
            );
        """)

        # 2. Products Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL CHECK(price > 0),
                stock INTEGER NOT NULL CHECK(stock >= 0)
            );
        """)

        # 3. Orders Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK(quantity > 0),
                total_price REAL NOT NULL CHECK(total_price > 0),
                order_date TEXT NOT NULL,
                status TEXT NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            );
        """)

        # 4. Payments Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                payment_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                payment_method TEXT NOT NULL,
                amount REAL NOT NULL CHECK(amount > 0),
                status TEXT NOT NULL,
                payment_date TEXT NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(order_id)
            );
        """)

        # 5. Reviews Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                review_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                rating INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5),
                review_text TEXT NOT NULL,
                review_date TEXT NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            );
        """)

        conn.commit()
        conn.close()

    def load_csv_data(self, raw_data_dir: str = os.path.join("data", "processed")) -> dict:
        """
        Loads CSV files into SQLite using explicit UPSERT (INSERT ... ON CONFLICT DO UPDATE).
        Returns a dictionary mapping table name to (csv_count, db_count).
        """
        self.create_tables()
        conn = self.get_connection()
        cursor = conn.cursor()

        load_specs = [
            (
                "customers.csv", "customers",
                ["customer_id", "name", "email", "created_at", "signup_channel"],
                "customer_id",
                "name=excluded.name, email=excluded.email, created_at=excluded.created_at, signup_channel=excluded.signup_channel"
            ),
            (
                "products.csv", "products",
                ["product_id", "name", "category", "price", "stock"],
                "product_id",
                "name=excluded.name, category=excluded.category, price=excluded.price, stock=excluded.stock"
            ),
            (
                "orders.csv", "orders",
                ["order_id", "customer_id", "product_id", "quantity", "total_price", "order_date", "status"],
                "order_id",
                "customer_id=excluded.customer_id, product_id=excluded.product_id, quantity=excluded.quantity, total_price=excluded.total_price, order_date=excluded.order_date, status=excluded.status"
            ),
            (
                "payments.csv", "payments",
                ["payment_id", "order_id", "payment_method", "amount", "status", "payment_date"],
                "payment_id",
                "order_id=excluded.order_id, payment_method=excluded.payment_method, amount=excluded.amount, status=excluded.status, payment_date=excluded.payment_date"
            ),
            (
                "reviews.csv", "reviews",
                ["review_id", "customer_id", "product_id", "rating", "review_text", "review_date"],
                "review_id",
                "customer_id=excluded.customer_id, product_id=excluded.product_id, rating=excluded.rating, review_text=excluded.review_text, review_date=excluded.review_date"
            )
        ]

        csv_counts = {}

        for file_name, table_name, columns, pk_col, update_clause in load_specs:
            file_path = os.path.join(raw_data_dir, file_name)
            if not os.path.exists(file_path):
                conn.close()
                raise FileNotFoundError(f"Processed CSV dataset missing: {file_path}")

            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            csv_counts[table_name] = len(rows)
            placeholders = ", ".join(["?"] * len(columns))
            cols_str = ", ".join(columns)
            
            # Explicit UPSERT Query
            query = f"""
                INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})
                ON CONFLICT({pk_col}) DO UPDATE SET {update_clause}
            """

            formatted_data = []
            for row in rows:
                record = []
                for col in columns:
                    val = row[col]
                    if col in ["price", "total_price", "amount"]:
                        val = float(val)
                    elif col in ["stock", "quantity", "rating"]:
                        val = int(val)
                    record.append(val)
                formatted_data.append(record)

            cursor.executemany(query, formatted_data)

        conn.commit()
        conn.close()

        # Dynamic validation: verify actual SQLite count == CSV count
        db_counts = self.get_row_counts()
        result_verification = {}
        for table, csv_c in csv_counts.items():
            db_c = db_counts[table]
            result_verification[table] = {"csv_count": csv_c, "db_count": db_c, "matched": csv_c == db_c}

        return result_verification

    def get_row_counts(self) -> dict:
        """Returns row counts for all 5 tables."""
        conn = self.get_connection()
        cursor = conn.cursor()
        counts = {}
        for table in ["customers", "products", "orders", "payments", "reviews"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
        conn.close()
        return counts

    def verify_foreign_keys(self) -> list:
        """Runs PRAGMA foreign_key_check and returns any violation tuples."""
        conn = self.get_connection()
        cursor = conn.cursor()
        violations = cursor.execute("PRAGMA foreign_key_check;").fetchall()
        conn.close()
        return violations
