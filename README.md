# SchemaIQ Backend — AI Database Intelligence API

> FastAPI + SQLAlchemy + Gemini API · Dynamic DB connections · AI-powered schema analysis

## 🚀 Quick Start

### 1. Set up Python environment
```bash
python -m venv venv

# Windows PowerShell
.\venv\Scripts\Activate.ps1
# macOS / Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env and add your Google AI API key
```

Your `.env` should look like:
```
GEMINI_API_KEY=AIzaSy...
```

### 3. Start the server
```bash
uvicorn main:app --reload --port 8001
```

Health check: http://localhost:8001/health

---

## 🌐 API Endpoints

| Method | Route | Description |
|--------|-------|-------------|
| `GET` | `/health` | Backend status + API key check |
| `POST` | `/api/connect` | Connect to a database via URL |
| `GET` | `/api/connection` | Current connection info |
| `POST` | `/api/upload` | Upload CSV / SQL file |
| `GET` | `/api/schema` | Extract schema (tables, columns, FKs) |
| `GET` | `/api/profile` | Statistical profiling of all columns |
| `GET` | `/api/dictionary/quick` | AI-generated data dictionary |
| `GET` | `/api/insights` | AI architectural insights |
| `GET` | `/api/suggestions` | AI-suggested questions |
| `POST` | `/chat` | Natural language → SQL → results |
| `GET/PUT` | `/api/settings` | Platform configuration |

---

## 🏗 Tech Stack

| Layer | Technology |
|-------|-----------|
| Framework | **FastAPI** |
| ORM / Inspector | **SQLAlchemy 2.0** |
| AI | **Google Gemini 2.5 Flash** |
| Data | **Pandas** |
| ML | **scikit-learn**, **NetworkX** |
| DB Drivers | **pymysql**, **psycopg2**, **pyodbc** |

---

## 🚢 Deploy to Render

This repo includes `render.yaml` for one-click Render deployment.

1. Create a **Web Service** on Render
2. Connect this repo
3. Set environment variables:
   - `GEMINI_API_KEY` — your Google AI API key
   - `FRONTEND_URL` — your Netlify frontend URL (for CORS)
4. Render will auto-detect `render.yaml` and deploy

**Start command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`

---

## 📁 Project Structure

```
├── main.py              # FastAPI app + routes
├── render.yaml          # Render deployment config
├── requirements.txt     # Python dependencies
├── .env.example         # API key template
├── routes/
│   ├── connection.py    # DB connection management
│   ├── schema.py        # Schema extraction
│   ├── profile.py       # Data profiling
│   ├── upload.py        # File upload (CSV/SQL)
│   ├── dictionary.py    # AI data dictionary
│   ├── insights.py      # AI insights
│   └── settings.py      # Platform settings
├── services/
│   ├── schema_extractor/
│   ├── relationship_mapper/
│   └── data_profiler/
├── db/
│   ├── connection.py    # Engine state management
│   ├── inspector.py     # SQLAlchemy inspection
│   └── loader.py        # CSV/SQL → SQLite loader
└── agent/
    ├── monitor.py       # TWIF anomaly detection
    ├── anomaly_model.py
    └── rules_model.py
```

---

*Built by Team Kaizen for Code Apex — Track 2: AI Agents*
