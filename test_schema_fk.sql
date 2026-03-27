PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS customers (
  customer_id TEXT PRIMARY KEY,
  customer_name TEXT
);

CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  order_date TEXT,
  total_amount REAL,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
