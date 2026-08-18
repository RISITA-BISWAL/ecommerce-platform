import csv
import os
import random
from datetime import datetime, timedelta

class EcommerceDataGenerator:
    """
    Generates synthetic e-commerce datasets (customers, products, orders, payments, reviews)
    with strict referential integrity and data quality validation.
    """
    def __init__(self, seed: int = 42):
        # Fixed seed ensures reproducible datasets
        random.seed(seed)
        
        self.first_names = [
            "Alex", "Jordan", "Taylor", "Morgan", "Sam", "Chris", "Pat", "Riley", "Casey", "Avery",
            "Logan", "Dakota", "Reese", "Skyler", "Rowan", "Finley", "Emerson", "Peyton", "Quinn", "Hayden"
        ]
        self.last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
            "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"
        ]
        self.channels = ["Web", "Mobile App", "Social Media", "Organic Search", "Referral"]
        
        self.categories = {
            "Electronics": [
                ("Wireless Headphones", 79.99), ("Smart Watch", 199.99), ("Mechanical Keyboard", 119.99),
                ("USB-C Hub", 34.99), ("Gaming Mouse", 49.99), ("Bluetooth Speaker", 59.99)
            ],
            "Clothing": [
                ("Denim Jacket", 59.99), ("Cotton T-Shirt", 19.99), ("Running Shoes", 89.99),
                ("Hoodie", 45.00), ("Leather Belt", 29.99), ("Wool Socks (3-Pack)", 14.99)
            ],
            "Home & Kitchen": [
                ("Coffee Maker", 49.99), ("Air Fryer", 89.99), ("Stainless Steel Water Bottle", 24.99),
                ("Desk Lamp", 29.99), ("Non-stick Frying Pan", 39.99), ("Blender", 69.99)
            ],
            "Books": [
                ("Python Data Engineering Handbook", 39.99), ("Designing Data-Intensive Applications", 49.99),
                ("Clean Code", 35.00), ("SQL for Data Analysis", 42.50), ("The Pragmatic Programmer", 44.99)
            ],
            "Beauty & Personal Care": [
                ("Sunscreen SPF 50", 15.99), ("Hydrating Facial Cleanser", 18.50), ("Electric Toothbrush", 64.99),
                ("Shampoo & Conditioner Set", 22.00)
            ],
            "Sports & Outdoors": [
                ("Yoga Mat", 25.00), ("Dumbbell Set (10 lbs)", 34.99), ("Camping Tent 4-Person", 129.99)
            ]
        }
        
        self.order_statuses = ["Completed", "Completed", "Completed", "Pending", "Cancelled", "Shipped"]
        self.payment_methods = ["Credit Card", "Debit Card", "PayPal", "UPI", "Apple Pay"]
        self.payment_statuses = ["Completed", "Completed", "Completed", "Pending", "Failed", "Refunded"]
        
        self.review_templates = {
            5: ["Exceptional quality! Exactly what I was looking for.", "Fast shipping and amazing product. Highly recommend!", "Works perfectly out of the box. 5 stars!"],
            4: ["Good quality product, reasonable price.", "Very satisfied overall, though shipping took an extra day.", "Solid build quality and performs well."],
            3: ["Average product. Meets expectations but nothing special.", "Decent for the price point.", "Works okay, but could be improved."],
            2: ["Disappointed with the quality.", "Smaller than expected and feels cheaply made.", "Item arrived slightly damaged."],
            1: ["Terrible experience, stopped working after two days.", "Poor quality, would not buy again.", "Did not match the description at all."]
        }

    def generate_customers(self, count: int = 100) -> list[dict]:
        """Generate a list of synthetic customers."""
        customers = []
        start_date = datetime.now() - timedelta(days=365)
        
        for i in range(1, count + 1):
            fname = random.choice(self.first_names)
            lname = random.choice(self.last_names)
            signup_date = start_date + timedelta(days=random.randint(0, 300))
            
            customer = {
                "customer_id": f"CUST-{i:04d}",
                "name": f"{fname} {lname}",
                "email": f"{fname.lower()}.{lname.lower()}{i}@example.com",
                "created_at": signup_date.strftime("%Y-%m-%d %H:%M:%S"),
                "signup_channel": random.choice(self.channels)
            }
            customers.append(customer)
            
        return customers

    def generate_products(self, count: int = 30) -> list[dict]:
        """Generate a list of synthetic products across multiple categories."""
        products = []
        product_id = 1
        
        for category, item_list in self.categories.items():
            for item_name, base_price in item_list:
                if product_id > count:
                    break
                product = {
                    "product_id": f"PROD-{product_id:04d}",
                    "name": item_name,
                    "category": category,
                    "price": round(base_price, 2),
                    "stock": random.randint(10, 200)
                }
                products.append(product)
                product_id += 1
                
        return products

    def generate_orders(self, count: int = 1000, customers: list[dict] = None, products: list[dict] = None) -> list[dict]:
        """Generate synthetic orders linked to valid customers and products."""
        if not customers:
            customers = self.generate_customers(100)
        if not products:
            products = self.generate_products(30)

        orders = []
        start_date = datetime.now() - timedelta(days=90)
        
        for i in range(1, count + 1):
            customer = random.choice(customers)
            product = random.choice(products)
            quantity = random.randint(1, 5)
            total_price = round(product["price"] * quantity, 2)
            order_date = start_date + timedelta(days=random.randint(0, 90), hours=random.randint(0, 23))
            
            order = {
                "order_id": f"ORD-{i:05d}",
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "total_price": total_price,
                "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "status": random.choice(self.order_statuses)
            }
            orders.append(order)
            
        return orders

    def generate_payments(self, orders: list[dict]) -> list[dict]:
        """Generate payment records for every order."""
        payments = []
        
        for i, order in enumerate(orders, start=1):
            order_dt = datetime.strptime(order["order_date"], "%Y-%m-%d %H:%M:%S")
            payment_dt = order_dt + timedelta(minutes=random.randint(1, 60))
            
            # Map order status to appropriate payment status logic
            if order["status"] == "Cancelled":
                pay_status = random.choice(["Failed", "Refunded"])
            elif order["status"] == "Pending":
                pay_status = "Pending"
            else:
                pay_status = "Completed"
                
            payment = {
                "payment_id": f"PAY-{i:05d}",
                "order_id": order["order_id"],
                "payment_method": random.choice(self.payment_methods),
                "amount": order["total_price"],
                "status": pay_status,
                "payment_date": payment_dt.strftime("%Y-%m-%d %H:%M:%S")
            }
            payments.append(payment)
            
        return payments

    def generate_reviews(self, count: int = 300, customers: list[dict] = None, products: list[dict] = None) -> list[dict]:
        """Generate synthetic reviews linked to valid customers and products."""
        if not customers:
            customers = self.generate_customers(100)
        if not products:
            products = self.generate_products(30)

        reviews = []
        start_date = datetime.now() - timedelta(days=80)
        
        for i in range(1, count + 1):
            customer = random.choice(customers)
            product = random.choice(products)
            rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 20, 35, 30])[0]
            review_text = random.choice(self.review_templates[rating])
            review_date = start_date + timedelta(days=random.randint(0, 80))
            
            review = {
                "review_id": f"REV-{i:05d}",
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "rating": rating,
                "review_text": review_text,
                "review_date": review_date.strftime("%Y-%m-%d %H:%M:%S")
            }
            reviews.append(review)
            
        return reviews

    @staticmethod
    def validate_datasets(customers: list[dict], products: list[dict], orders: list[dict], payments: list[dict], reviews: list[dict]) -> dict:
        """
        Validates datasets for missing values, duplicates, numeric constraints, and foreign key integrity.
        Returns a summary report dict.
        Raises ValueError if validation fails.
        """
        report = {"errors": [], "checks_passed": True}
        
        # 1. Primary ID sets
        cust_ids = {c["customer_id"] for c in customers}
        prod_ids = {p["product_id"] for p in products}
        ord_ids = {o["order_id"] for o in orders}
        pay_ids = {p["payment_id"] for p in payments}
        rev_ids = {r["review_id"] for r in reviews}
        
        # Check duplicate primary IDs
        for name, records, key in [
            ("customers", customers, "customer_id"),
            ("products", products, "product_id"),
            ("orders", orders, "order_id"),
            ("payments", payments, "payment_id"),
            ("reviews", reviews, "review_id")
        ]:
            ids = [r[key] for r in records]
            if len(ids) != len(set(ids)):
                report["errors"].append(f"Duplicate primary IDs found in {name} dataset.")
                
        # 2. Check null values
        for name, records in [("customers", customers), ("products", products), ("orders", orders), ("payments", payments), ("reviews", reviews)]:
            for r in records:
                if any(v is None or v == "" for v in r.values()):
                    report["errors"].append(f"Missing or null value detected in {name} dataset.")
                    break

        # 3. Numeric constraints
        for p in products:
            if p["price"] <= 0:
                report["errors"].append(f"Invalid product price ({p['price']}) in product {p['product_id']}.")

        for o in orders:
            if o["quantity"] <= 0:
                report["errors"].append(f"Invalid quantity ({o['quantity']}) in order {o['order_id']}.")
            if o["total_price"] <= 0:
                report["errors"].append(f"Invalid total_price ({o['total_price']}) in order {o['order_id']}.")

        for p in payments:
            if p["amount"] <= 0:
                report["errors"].append(f"Invalid payment amount ({p['amount']}) in payment {p['payment_id']}.")

        for r in reviews:
            if not (1 <= r["rating"] <= 5):
                report["errors"].append(f"Invalid rating ({r['rating']}) in review {r['review_id']}.")

        # 4. Foreign key integrity
        for o in orders:
            if o["customer_id"] not in cust_ids:
                report["errors"].append(f"Broken FK: orders.customer_id ({o['customer_id']}) not found in customers.")
            if o["product_id"] not in prod_ids:
                report["errors"].append(f"Broken FK: orders.product_id ({o['product_id']}) not found in products.")

        for p in payments:
            if p["order_id"] not in ord_ids:
                report["errors"].append(f"Broken FK: payments.order_id ({p['order_id']}) not found in orders.")

        for r in reviews:
            if r["customer_id"] not in cust_ids:
                report["errors"].append(f"Broken FK: reviews.customer_id ({r['customer_id']}) not found in customers.")
            if r["product_id"] not in prod_ids:
                report["errors"].append(f"Broken FK: reviews.product_id ({r['product_id']}) not found in products.")

        if report["errors"]:
            report["checks_passed"] = False
            raise ValueError(f"Dataset Validation Failed with {len(report['errors'])} errors: {report['errors']}")

        return report

    @staticmethod
    def save_to_csv(data: list[dict], file_path: str) -> None:
        """Save list of dictionaries as CSV file."""
        if not data:
            return
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        fieldnames = data[0].keys()
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
