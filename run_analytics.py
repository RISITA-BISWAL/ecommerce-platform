import os
from src.analytics import EcommerceAnalytics

def main():
    db_path = os.path.join("data", "ecommerce.db")
    if not os.path.exists(db_path):
        print(f"[ERROR] Database file '{db_path}' not found! Please run 'python run_pipeline.py' first.")
        return

    print("==================================================================")
    print("        E-COMMERCE DATA PLATFORM - SQL ANALYTICS REPORT        ")
    print("==================================================================\n")

    analytics = EcommerceAnalytics(db_path=db_path)

    # 1. Executive KPIs
    print("--- [1. EXECUTIVE KPI SUMMARY] ---")
    kpis = analytics.get_executive_kpis()
    for col in kpis.columns:
        print(f"  - {col:25s}: {kpis[col].iloc[0]}")
    print()

    # 2. Revenue Reconciliation
    print("--- [2. REVENUE RECONCILIATION REPORT] ---")
    reconciliation = analytics.reconcile_revenue()
    if len(reconciliation) == 0:
        print("  [SUCCESS] 0 Discrepancies Found! (Order total_price matches quantity * unit_price 100%)\n")
    else:
        print(f"  [WARNING] {len(reconciliation)} order revenue discrepancies detected!")
        print(reconciliation.to_string(index=False))
        print()

    # 3. Sales Analytics
    print("--- [3. SALES ANALYTICS] ---")
    print("Order Status Breakdown:")
    print(analytics.get_orders_by_status().to_string(index=False))
    print()

    # 4. Product Analytics
    print("--- [4. PRODUCT ANALYTICS] ---")
    print("Revenue by Product Category:")
    print(analytics.get_revenue_by_category().to_string(index=False))
    print("\nTop 5 Selling Products:")
    print(analytics.get_top_selling_products(limit=5).to_string(index=False))
    print()

    # 5. Customer Analytics
    print("--- [5. CUSTOMER ANALYTICS] ---")
    print("Top 5 Spending Customers:")
    print(analytics.get_customer_analytics(limit=5).to_string(index=False))
    print()

    # 6. Payment Analytics
    print("--- [6. PAYMENT ANALYTICS] ---")
    print("Payment Method Distribution:")
    print(analytics.get_payment_method_distribution().to_string(index=False))
    print()

    # 7. Review Analytics
    print("--- [7. REVIEW ANALYTICS] ---")
    print("Rating Sentiment Breakdown:")
    print(analytics.get_rating_category_distribution().to_string(index=False))
    print()

    # 8. Time-based Analytics
    print("--- [8. TIME-BASED MONTHLY TRENDS] ---")
    print(analytics.get_monthly_trends().to_string(index=False))
    print("\n==================================================================")
    print("                   END OF ANALYTICS REPORT                       ")
    print("==================================================================")

if __name__ == "__main__":
    main()
