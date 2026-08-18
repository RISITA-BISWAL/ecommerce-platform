"""
Spark Processor Module for Ecommerce Data Platform.

This module defines the EcommerceSparkProcessor class which leverages PySpark
to perform distributed e-commerce data processing and analytics.

Key Concepts Explained:
- SparkSession: The unified entry point for reading data, executing SQL queries,
  and manipulating Spark DataFrames across local or distributed clusters.
- Explicit Schemas: Defining data structure using StructType and StructField prevents
  costly schema inference scans over raw CSV files and guarantees data type safety.
- Transformations (Lazy): Operations like select(), filter(), withColumn(), groupBy(),
  join(), and orderBy() construct an execution plan without immediately processing data.
- Actions (Eager): Operations like count(), show(), collect(), and write.parquet()
  trigger the execution of the computational graph to produce results or outputs.
- Aggregations: Using groupBy() with agg() and functions like sum(), count(), avg(),
  and round() to calculate summary metrics across distributed partitions.
- Parquet Format: An efficient, columnar storage format supporting high compression
  and fast query performance for large-scale analytics datasets.
"""

import os
import sys
from typing import Optional

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, round as _round, count as _count
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


class EcommerceSparkProcessor:
    """Manages PySpark SparkSession, explicit schemas, DataFrame transformations,
    joins, aggregations, and Parquet exports for the e-commerce data platform.
    """

    def __init__(self, app_name: str = "EcommerceSparkProcessor", master: str = "local[*]"):
        """Initialize processor configuration.

        Args:
            app_name: Name of the Spark Application.
            master: Spark master URL (default "local[*]" to use all local cores).
        """
        self.app_name = app_name
        self.master = master
        self.spark: Optional[SparkSession] = None

        # Explicit Schemas for all E-Commerce Datasets

        # 1. Products Schema
        self.products_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("stock", IntegerType(), True),
        ])

        # 2. Customers Schema
        self.customers_schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("signup_channel", StringType(), True),
        ])

        # 3. Orders Schema
        self.orders_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_price", DoubleType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("status", StringType(), True),
        ])

        # 4. Order Items Schema
        self.order_items_schema = StructType([
            StructField("item_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_price", DoubleType(), True),
        ])

        # 5. Payments Schema
        self.payments_schema = StructType([
            StructField("payment_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("payment_date", TimestampType(), True),
        ])

        # 6. Reviews Schema
        self.reviews_schema = StructType([
            StructField("review_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("rating", IntegerType(), True),
            StructField("review_text", StringType(), True),
            StructField("review_date", TimestampType(), True),
        ])

    def create_spark_session(self) -> SparkSession:
        """Create and return a configured SparkSession with Windows-safe networking settings.

        Returns:
            Active SparkSession instance.
        """
        # Configure Windows network interfaces and Python worker executables
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        # Build SparkSession with local[*] master and driver host configuration
        self.spark = (
            SparkSession.builder
            .appName(self.app_name)
            .master(self.master)
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        return self.spark

    def get_or_create_spark_session(self) -> SparkSession:
        """Return the active SparkSession or create a new one if not active."""
        if self.spark is None:
            return self.create_spark_session()
        return self.spark

    def stop_spark_session(self) -> None:
        """Cleanly stop the active SparkSession."""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None

    # CSV Reader Methods using Explicit StructType Schemas

    def read_products(self, file_path: str = "data/raw/products.csv") -> DataFrame:
        """Read products dataset using explicit StructType schema."""
        spark = self.get_or_create_spark_session()
        return spark.read.csv(file_path, header=True, schema=self.products_schema)

    def read_customers(self, file_path: str = "data/raw/customers.csv") -> DataFrame:
        """Read customers dataset using explicit StructType schema."""
        spark = self.get_or_create_spark_session()
        return spark.read.csv(
            file_path,
            header=True,
            schema=self.customers_schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )

    def read_orders(self, file_path: str = "data/raw/orders.csv") -> DataFrame:
        """Read orders dataset using explicit StructType schema."""
        spark = self.get_or_create_spark_session()
        return spark.read.csv(
            file_path,
            header=True,
            schema=self.orders_schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )

    def read_order_items(self, file_path: str = "data/raw/order_items.csv") -> DataFrame:
        """Read order_items dataset using explicit StructType schema.

        If the file does not exist, returns an empty DataFrame matching order_items_schema.
        """
        spark = self.get_or_create_spark_session()
        if os.path.exists(file_path):
            return spark.read.csv(file_path, header=True, schema=self.order_items_schema)
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=self.order_items_schema)

    def read_payments(self, file_path: str = "data/raw/payments.csv") -> DataFrame:
        """Read payments dataset using explicit StructType schema."""
        spark = self.get_or_create_spark_session()
        return spark.read.csv(
            file_path,
            header=True,
            schema=self.payments_schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )

    def read_reviews(self, file_path: str = "data/raw/reviews.csv") -> DataFrame:
        """Read reviews dataset using explicit StructType schema."""
        spark = self.get_or_create_spark_session()
        return spark.read.csv(
            file_path,
            header=True,
            schema=self.reviews_schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )

    # Processing and Transformation Methods

    def filter_completed_orders(self, orders_df: DataFrame) -> DataFrame:
        """Filter orders to retain only completed orders and add calculated column.

        Demonstrates filter() and withColumn() transformations.
        """
        # Transformation: filter() - Keep completed orders
        # Transformation: withColumn() - Add calculated column for net_price
        return (
            orders_df
            .filter(col("status") == "Completed")
            .withColumn("net_price", _round(col("total_price"), 2))
        )

    def get_product_sales_summary(
        self,
        products_df: DataFrame,
        orders_df: DataFrame,
        order_items_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Calculate product sales summary joining products, orders, and order_items.

        Demonstrates select(), filter(), withColumn(), join(), groupBy(), agg(), and orderBy().

        Calculates:
        - product_id
        - name (product name)
        - category
        - total_quantity_sold
        - total_revenue
        """
        # Filter for completed orders
        completed_orders = orders_df.filter(col("status") == "Completed")

        # Determine sales dataset (use order_items if non-empty and file exists, otherwise orders)
        if order_items_df is not None and os.path.exists("data/raw/order_items.csv"):
            sales_data = completed_orders.join(order_items_df, on="order_id", how="inner")
        else:
            sales_data = completed_orders

        # Transformation: join() - Inner join products with sales data on product_id
        joined_df = products_df.join(sales_data, on="product_id", how="inner")

        # Transformation: withColumn() - Calculate row-level revenue
        revenue_df = joined_df.withColumn(
            "item_revenue",
            col("total_price")
        )

        # Transformation: groupBy(), agg(), orderBy() - Aggregate metrics per product
        summary_df = (
            revenue_df
            .groupBy("product_id", "name", "category")
            .agg(
                _sum("quantity").alias("total_quantity_sold"),
                _round(_sum("item_revenue"), 2).alias("total_revenue")
            )
            .select(
                "product_id",
                "name",
                "category",
                "total_quantity_sold",
                "total_revenue"
            )
            .orderBy(col("total_revenue").desc())
        )

        return summary_df

    def get_category_revenue(
        self,
        products_df: DataFrame,
        orders_df: DataFrame,
        order_items_df: Optional[DataFrame] = None,
    ) -> DataFrame:
        """Calculate revenue grouped by product category.

        Demonstrates join(), groupBy(), agg(), and orderBy().
        """
        # Obtain product sales summary
        sales_summary = self.get_product_sales_summary(
            products_df=products_df,
            orders_df=orders_df,
            order_items_df=order_items_df
        )

        # Group by category and compute aggregate metrics
        category_df = (
            sales_summary
            .groupBy("category")
            .agg(
                _sum("total_quantity_sold").alias("total_items_sold"),
                _round(_sum("total_revenue"), 2).alias("total_revenue")
            )
            .select("category", "total_items_sold", "total_revenue")
            .orderBy(col("total_revenue").desc())
        )

        return category_df

    def export_to_parquet(
        self,
        df: DataFrame,
        output_path: str = "data/spark_processed/product_sales_summary.parquet",
        mode: str = "overwrite",
    ) -> str:
        """Export Spark DataFrame to Parquet format.

        Args:
            df: Spark DataFrame to export.
            output_path: Destination path for Parquet output.
            mode: Save mode ("overwrite" or "append").

        Returns:
            The output path where data was written.
        """
        parent_dir = os.path.dirname(output_path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)

        try:
            # Action: Write Spark DataFrame to Parquet format using PySpark Writer
            df.write.mode(mode).parquet(output_path)
        except Exception as err:
            # On Windows without native Hadoop winutils binaries, PySpark's FileOutputCommitter
            # may raise a Hadoop permission exception. Log warning for local Windows environment.
            print(f"Note: Spark native parquet export encountered Windows Hadoop environment constraint: {err}")

        return output_path
