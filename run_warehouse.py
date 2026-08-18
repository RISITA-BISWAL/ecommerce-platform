"""
Standalone execution runner for Milestone 10 Data Warehouse & Star Schema Engine.
"""

from src.warehouse import EcommerceDataWarehouse

def main():
    print("[WAREHOUSE] Starting Data Warehouse & Star Schema Builder...")
    
    dw = EcommerceDataWarehouse()
    summary = dw.build_full_warehouse()

    print("[WAREHOUSE] ELT Build Complete:")
    print(f"  - dim_date records     : {summary['dim_date_records']}")
    print(f"  - dim_customer records : {summary['dim_customer_records']}")
    print(f"  - dim_product records  : {summary['dim_product_records']}")
    print(f"  - fact_sales records   : {summary['fact_sales_records']}")

    print("\n[OLAP ANALYTICS] Category Revenue Drilldown (Year / Quarter):")
    df_category = dw.get_category_revenue_drilldown()
    print(df_category.head(10).to_string(index=False))

    print("\n[OLAP ANALYTICS] Weekend vs Weekday Sales Performance:")
    df_weekend = dw.get_weekend_sales_performance()
    print(df_weekend.to_string(index=False))

if __name__ == "__main__":
    main()
