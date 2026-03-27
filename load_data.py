"""
load_data.py — Load Olist CSV files into SQLite

Usage:
    python load_data.py --data ./data

Expects this folder structure:
    data/
      olist_customers_dataset.csv
      olist_orders_dataset.csv
      olist_order_items_dataset.csv
      olist_order_payments_dataset.csv
      olist_order_reviews_dataset.csv
      olist_products_dataset.csv
      olist_sellers_dataset.csv
      olist_geolocation_dataset.csv
      product_category_name_translation.csv

Download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
"""

import argparse
import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / "olist.db"

# Map: CSV filename → SQLite table name
CSV_MAP = {
    "olist_customers_dataset.csv":             "customers",
    "olist_orders_dataset.csv":                "orders",
    "olist_order_items_dataset.csv":           "order_items",
    "olist_order_payments_dataset.csv":        "order_payments",
    "olist_order_reviews_dataset.csv":         "order_reviews",
    "olist_products_dataset.csv":              "products",
    "olist_sellers_dataset.csv":               "sellers",
    "olist_geolocation_dataset.csv":           "geolocation",
    "product_category_name_translation.csv":   "product_category_name_translation",
}

def load(data_dir: str):
    data_path = Path(data_dir)
    if not data_path.exists():
        raise FileNotFoundError(f"Data directory not found: {data_path}")

    # Remove existing DB
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"🗑  Removed existing {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    total_rows = 0

    for csv_file, table_name in CSV_MAP.items():
        csv_path = data_path / csv_file
        if not csv_path.exists():
            print(f"⚠  Skipping {csv_file} (not found)")
            continue

        print(f"⏳ Loading {csv_file} → {table_name}...", end=" ", flush=True)
        df = pd.read_csv(csv_path, low_memory=False)

        # Write to SQLite
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        rows = len(df)
        total_rows += rows
        print(f"✓  {rows:,} rows")

    # Create indexes for common joins
    print("\n📐 Creating indexes...")
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_orders_customer    ON orders(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_status      ON orders(order_status)",
        "CREATE INDEX IF NOT EXISTS idx_orders_purchase_ts ON orders(order_purchase_timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_items_order        ON order_items(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_items_product      ON order_items(product_id)",
        "CREATE INDEX IF NOT EXISTS idx_items_seller       ON order_items(seller_id)",
        "CREATE INDEX IF NOT EXISTS idx_payments_order     ON order_payments(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_reviews_order      ON order_reviews(order_id)",
    ]
    for idx in indexes:
        conn.execute(idx)
    conn.commit()
    conn.close()

    db_size = DB_PATH.stat().st_size / (1024 * 1024)
    print(f"\n✅ Done! {total_rows:,} total rows → {DB_PATH} ({db_size:.1f} MB)")
    print("\nTables loaded:")
    conn2 = sqlite3.connect(DB_PATH)
    for row in conn2.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        count = conn2.execute(f"SELECT COUNT(*) FROM {row[0]}").fetchone()[0]
        print(f"  {row[0]:<45} {count:>10,} rows")
    conn2.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Olist CSVs into SQLite")
    parser.add_argument("--data", default="./data", help="Path to folder containing Olist CSVs")
    args = parser.parse_args()
    load(args.data)
