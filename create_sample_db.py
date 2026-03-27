#!/usr/bin/env python3
"""
Create a sample database with realistic e-commerce data to test the AI dictionary
"""

import sqlite3
import random
from datetime import datetime, timedelta

def create_sample_database():
    # Connect to SQLite database
    conn = sqlite3.connect('sample_ecommerce.db')
    cursor = conn.cursor()
    
    # Create tables with realistic e-commerce schema
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        phone VARCHAR(20),
        registration_date DATE,
        loyalty_tier VARCHAR(20),
        total_spent DECIMAL(10,2) DEFAULT 0.00,
        is_active BOOLEAN DEFAULT 1
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS product_categories (
        category_id INTEGER PRIMARY KEY,
        category_name VARCHAR(100) NOT NULL,
        parent_category_id INTEGER,
        description TEXT,
        is_active BOOLEAN DEFAULT 1,
        FOREIGN KEY (parent_category_id) REFERENCES product_categories(category_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        sku VARCHAR(50) UNIQUE NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        category_id INTEGER,
        price DECIMAL(10,2) NOT NULL,
        cost DECIMAL(10,2),
        stock_quantity INTEGER DEFAULT 0,
        weight_kg DECIMAL(8,3),
        dimensions VARCHAR(50),
        brand VARCHAR(100),
        description TEXT,
        created_date DATE,
        is_active BOOLEAN DEFAULT 1,
        FOREIGN KEY (category_id) REFERENCES product_categories(category_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        order_date DATETIME,
        status VARCHAR(20),
        total_amount DECIMAL(10,2),
        shipping_cost DECIMAL(8,2),
        tax_amount DECIMAL(8,2),
        discount_amount DECIMAL(8,2) DEFAULT 0.00,
        payment_method VARCHAR(50),
        shipping_address TEXT,
        billing_address TEXT,
        tracking_number VARCHAR(100),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS order_items (
        item_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        total_price DECIMAL(10,2) NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS inventory_transactions (
        transaction_id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        transaction_type VARCHAR(20),
        quantity_change INTEGER,
        reference_order_id INTEGER,
        transaction_date DATETIME,
        notes TEXT,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (reference_order_id) REFERENCES orders(order_id)
    )
    ''')
    
    # Insert sample data
    
    # Categories
    categories = [
        (1, 'Electronics', None, 'Electronic devices and accessories', 1),
        (2, 'Clothing', None, 'Fashion and apparel', 1),
        (3, 'Home & Garden', None, 'Home improvement and gardening', 1),
        (4, 'Smartphones', 1, 'Mobile phones and accessories', 1),
        (5, 'Laptops', 1, 'Portable computers', 1),
        (6, "Men's Clothing", 2, 'Clothing for men', 1),
        (7, "Women's Clothing", 2, 'Clothing for women', 1)
    ]
    
    cursor.executemany('''
    INSERT OR REPLACE INTO product_categories 
    (category_id, category_name, parent_category_id, description, is_active) 
    VALUES (?, ?, ?, ?, ?)
    ''', categories)
    
    # Customers
    customers = []
    for i in range(1, 101):
        customers.append((
            i,
            f'customer{i}@email.com',
            f'FirstName{i}',
            f'LastName{i}',
            f'+1-555-{1000+i:04d}',
            (datetime.now() - timedelta(days=random.randint(1, 365))).date(),
            random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            round(random.uniform(50.0, 5000.0), 2),
            random.choice([1, 1, 1, 0])  # 75% active
        ))
    
    cursor.executemany('''
    INSERT OR REPLACE INTO customers 
    (customer_id, email, first_name, last_name, phone, registration_date, loyalty_tier, total_spent, is_active) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', customers)
    
    # Products
    products = [
        (1, 'IPHONE-15-PRO', 'iPhone 15 Pro 256GB', 4, 1199.99, 800.00, 25, 0.221, '146.6×70.6×8.25mm', 'Apple', 'Latest iPhone with Pro camera system', datetime.now().date(), 1),
        (2, 'SAMSUNG-S24', 'Samsung Galaxy S24 Ultra', 4, 1299.99, 850.00, 18, 0.232, '162.3×79.0×8.6mm', 'Samsung', 'Premium Android smartphone', datetime.now().date(), 1),
        (3, 'MACBOOK-PRO-M3', 'MacBook Pro 14" M3', 5, 1999.99, 1400.00, 12, 1.55, '312.6×221.2×15.5mm', 'Apple', 'Professional laptop with M3 chip', datetime.now().date(), 1),
        (4, 'DELL-XPS-13', 'Dell XPS 13 Plus', 5, 1299.99, 900.00, 8, 1.26, '295.3×199.0×15.28mm', 'Dell', 'Ultra-portable business laptop', datetime.now().date(), 1),
        (5, 'NIKE-AIR-MAX', 'Nike Air Max 270', 6, 149.99, 75.00, 45, 0.8, 'Size 10', 'Nike', 'Comfortable running shoes', datetime.now().date(), 1),
        (6, 'LEVI-JEANS-501', 'Levi\'s 501 Original Jeans', 6, 89.99, 35.00, 32, 0.6, 'W32 L34', 'Levi\'s', 'Classic straight-leg jeans', datetime.now().date(), 1),
        (7, 'ZARA-DRESS-BLK', 'Zara Black Evening Dress', 7, 79.99, 25.00, 15, 0.3, 'Size M', 'Zara', 'Elegant evening dress', datetime.now().date(), 1),
        (8, 'DYSON-V15', 'Dyson V15 Detect Vacuum', 3, 749.99, 400.00, 6, 3.1, '1257×250×166mm', 'Dyson', 'Cordless vacuum with laser detection', datetime.now().date(), 1)
    ]
    
    cursor.executemany('''
    INSERT OR REPLACE INTO products 
    (product_id, sku, product_name, category_id, price, cost, stock_quantity, weight_kg, dimensions, brand, description, created_date, is_active) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', products)
    
    # Orders
    orders = []
    for i in range(1, 51):
        orders.append((
            i,
            random.randint(1, 100),  # customer_id
            datetime.now() - timedelta(days=random.randint(1, 90)),
            random.choice(['pending', 'processing', 'shipped', 'delivered', 'cancelled']),
            round(random.uniform(50.0, 2000.0), 2),
            round(random.uniform(5.0, 25.0), 2),
            round(random.uniform(5.0, 150.0), 2),
            round(random.uniform(0.0, 100.0), 2),
            random.choice(['credit_card', 'paypal', 'apple_pay', 'bank_transfer']),
            f'123 Main St, City {i}, State, 12345',
            f'456 Billing Ave, City {i}, State, 12345',
            f'TRK{1000000 + i}'
        ))
    
    cursor.executemany('''
    INSERT OR REPLACE INTO orders 
    (order_id, customer_id, order_date, status, total_amount, shipping_cost, tax_amount, discount_amount, payment_method, shipping_address, billing_address, tracking_number) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', orders)
    
    # Order Items
    order_items = []
    item_id = 1
    for order_id in range(1, 51):
        num_items = random.randint(1, 4)
        for _ in range(num_items):
            product_id = random.randint(1, 8)
            quantity = random.randint(1, 3)
            # Get product price (simplified - using fixed prices)
            product_prices = {1: 1199.99, 2: 1299.99, 3: 1999.99, 4: 1299.99, 5: 149.99, 6: 89.99, 7: 79.99, 8: 749.99}
            unit_price = product_prices.get(product_id, 100.0)
            total_price = unit_price * quantity
            
            order_items.append((
                item_id,
                order_id,
                product_id,
                quantity,
                unit_price,
                total_price
            ))
            item_id += 1
    
    cursor.executemany('''
    INSERT OR REPLACE INTO order_items 
    (item_id, order_id, product_id, quantity, unit_price, total_price) 
    VALUES (?, ?, ?, ?, ?, ?)
    ''', order_items)
    
    # Inventory Transactions
    transactions = []
    for i in range(1, 101):
        transactions.append((
            i,
            random.randint(1, 8),  # product_id
            random.choice(['stock_in', 'stock_out', 'adjustment', 'return']),
            random.randint(-10, 50),
            random.randint(1, 50) if random.random() > 0.3 else None,
            datetime.now() - timedelta(days=random.randint(1, 30)),
            f'Transaction note {i}'
        ))
    
    cursor.executemany('''
    INSERT OR REPLACE INTO inventory_transactions 
    (transaction_id, product_id, transaction_type, quantity_change, reference_order_id, transaction_date, notes) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', transactions)
    
    conn.commit()
    conn.close()
    print("✅ Sample e-commerce database created successfully: sample_ecommerce.db")
    print("📊 Contains: customers, products, orders, categories, inventory with realistic data")

if __name__ == "__main__":
    create_sample_database()
