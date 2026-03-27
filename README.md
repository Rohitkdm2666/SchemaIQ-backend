# SchemaIQ Backend — Setup Guide

## Architecture

```
React Frontend (port 5173)
        ↓ POST /chat
FastAPI Backend (port 8000)
        ↓
Claude API (generates SQL)
        ↓
SQLite DB (Olist CSVs)
        ↓
Real query results → Claude formats → Frontend renders
```

---

## Step 1 — Download the Olist Dataset

1. Go to: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Download and extract the ZIP
3. Create a `data/` folder inside `schemaiq-backend/`
4. Copy all 9 CSV files into `schemaiq-backend/data/`

You need these files:
```
schemaiq-backend/data/
  olist_customers_dataset.csv
  olist_orders_dataset.csv
  olist_order_items_dataset.csv
  olist_order_payments_dataset.csv
  olist_order_reviews_dataset.csv
  olist_products_dataset.csv
  olist_sellers_dataset.csv
  olist_geolocation_dataset.csv
  product_category_name_translation.csv
```

---

## Step 2 — Set up Python environment

```powershell
cd schemaiq-backend

# Create virtual environment
python -m venv venv

# Activate it (Windows PowerShell)
.\venv\Scripts\Activate.ps1

# Activate it (Mac/Linux)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Step 3 — Set your API key

```powershell
# Copy the example env file
copy .env.example .env

# Open .env and paste your Anthropic API key
# Get one from: https://console.anthropic.com/
```

Your `.env` should look like:
```
ANTHROPIC_API_KEY=sk-ant-api03-...
```

---

## Step 4 — Load the Olist CSVs into SQLite

```powershell
python load_data.py --data ./data
```

Expected output:
```
⏳ Loading olist_customers_dataset.csv → customers... ✓  99,441 rows
⏳ Loading olist_orders_dataset.csv → orders...       ✓  99,441 rows
⏳ Loading olist_order_items_dataset.csv → order_items... ✓  112,650 rows
...
✅ Done! 1,618,689 total rows → olist.db (142.3 MB)
```

This creates `olist.db` — a local SQLite database with all 9 tables and indexes.

---

## Step 5 — Start the backend

```powershell
# Make sure venv is activated
uvicorn main:app --reload --port 8000
```

Check it's running: http://localhost:8000
Health check: http://localhost:8000/health

Expected:
```json
{"db": true, "anthropic_api": true, "ready": true}
```

---

## Step 6 — Start the frontend

In a separate terminal:
```powershell
cd schemaiq
npm run dev
```

Open http://localhost:5173, go to QueryBot, and ask anything!

---

## Example queries that work

| Question | What it does |
|---|---|
| How many orders were delivered in 2018? | COUNT with date filter |
| Top 5 product categories by revenue | GROUP BY + SUM + ORDER |
| Which state has the most customers? | GROUP BY state |
| Average review score by category | JOIN 4 tables |
| Total GMV of all delivered orders | SUM price + freight |
| How many sellers are in São Paulo? | WHERE state = 'SP' |
| Orders canceled in March 2018 | COUNT with date + status filter |
| Most popular payment method | GROUP BY payment_type |

---

## Troubleshooting

**"Database not found"**
→ Run `python load_data.py --data ./data`

**"ANTHROPIC_API_KEY not set"**
→ Create `.env` file with your key

**"Backend not reachable"**
→ Make sure `uvicorn main:app --reload` is running

**CORS error in browser**
→ Backend already allows `localhost:5173` — make sure both servers are running

**SQL error from Claude**
→ Backend auto-retries once. If it keeps failing, the question may be too ambiguous — try rephrasing.

---

## File structure

```
schemaiq-backend/
├── main.py          ← FastAPI server + Claude integration
├── load_data.py     ← CSV → SQLite loader
├── requirements.txt ← Python dependencies
├── .env.example     ← API key template
├── .env             ← Your actual API key (git-ignored)
├── olist.db         ← Generated SQLite DB (after load_data.py)
└── data/            ← Olist CSV files go here
    ├── olist_customers_dataset.csv
    └── ...
```
