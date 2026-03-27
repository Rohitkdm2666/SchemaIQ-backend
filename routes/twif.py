"""
TWIF Model Routes
API endpoints for TWIF model training and anomaly detection
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import logging
from pathlib import Path
import json
import numpy as np
from fastapi.responses import JSONResponse

from services.twif_training import TWIFTrainingService
from services.twif_anomaly import TWIFAnomalyService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("routes.twif")
router = APIRouter()

# Initialize services
training_service = TWIFTrainingService()
anomaly_service = TWIFAnomalyService()

# Pydantic models
class TrainingRequest(BaseModel):
    db_path: str
    model_name: Optional[str] = "twif_default"
    n_estimators: Optional[int] = 150
    topology_weight: Optional[float] = 0.4
    relationship_boost: Optional[float] = 1.8
    contamination: Optional[str] = "auto"
    random_state: Optional[int] = 42

class AnomalyDetectionRequest(BaseModel):
    db_path: str
    model_name: Optional[str] = "twif_default"
    anomaly_threshold: Optional[float] = None

class TableAnalysisRequest(BaseModel):
    db_path: str
    table_name: str
    model_name: Optional[str] = "twif_default"

class DemoRequest(BaseModel):
    db_path: str


def _resolve_demo_db_path(requested_path: str) -> Path:
    """
    Resolve db_path robustly for Windows/relative demo calls.
    Supports:
    - absolute path
    - relative path from current working directory
    - relative path from Schema Intelligence Engine directory
    - common shorthand: "olist.db" located under backend/
    """
    raw = (requested_path or "").strip().strip('"').strip("'")
    normalized = raw.replace("\\", "/")
    candidate_input = Path(normalized)

    # Absolute path: return as-is if present
    if candidate_input.is_absolute() and candidate_input.exists():
        return candidate_input

    sie_dir = Path(__file__).resolve().parents[1]        # .../Schema Intelligence Engine
    backend_dir = Path(__file__).resolve().parents[2]    # .../backend

    candidates = [
        Path.cwd() / candidate_input,
        sie_dir / candidate_input,
        backend_dir / candidate_input,
    ]

    # Demo-friendly fallback for shorthand "olist.db"
    if candidate_input.name.lower() == "olist.db":
        candidates.append(backend_dir / "olist.db")

    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()

    searched = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(
        f"Database not found for requested path '{requested_path}'. Searched: {searched}"
    )


def _to_json_safe(value: Any) -> Any:
    """Convert numpy/scalar containers into JSON-safe Python primitives."""
    if isinstance(value, dict):
        return {str(k): _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_to_json_safe(v) for v in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    return value

# Training endpoints
@router.post("/twif/train")
async def train_twif_model(request: TrainingRequest, background_tasks: BackgroundTasks):
    """
    Train TWIF model on database
    
    Args:
        request: Training parameters and database path
    
    Returns:
        Training results and model information
    """
    try:
        logger.info(f"Starting TWIF training: {request.model_name}")
        
        # Start training in background for long-running tasks
        if request.model_name == "twif_background":
            background_tasks.add_task(
                training_service.train_model,
                db_path=request.db_path,
                model_name=request.model_name,
                n_estimators=request.n_estimators,
                topology_weight=request.topology_weight,
                relationship_boost=request.relationship_boost,
                contamination=request.contamination,
                random_state=request.random_state
            )
            
            return {
                "status": "training_started",
                "model_name": request.model_name,
                "message": "Training started in background"
            }
        
        # Synchronous training for immediate results
        results = training_service.train_model(
            db_path=request.db_path,
            model_name=request.model_name,
            n_estimators=request.n_estimators,
            topology_weight=request.topology_weight,
            relationship_boost=request.relationship_boost,
            contamination=request.contamination,
            random_state=request.random_state
        )
        
        return {
            "status": "success",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"TWIF training error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Training failed: {str(e)}")

@router.get("/twif/models")
async def list_twif_models():
    """List all trained TWIF models"""
    try:
        models = training_service.list_models()
        return {
            "status": "success",
            "models": models
        }
    except Exception as e:
        logger.error(f"Error listing models: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list models: {str(e)}")

@router.get("/twif/models/{model_name}")
async def get_model_info(model_name: str):
    """Get detailed information about a specific TWIF model"""
    try:
        model_info = training_service.get_model_info(model_name)
        
        if model_info is None:
            raise HTTPException(status_code=404, detail=f"Model not found: {model_name}")
        
        return {
            "status": "success",
            "model_info": model_info
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting model info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get model info: {str(e)}")

@router.delete("/twif/models/{model_name}")
async def delete_model(model_name: str):
    """Delete a trained TWIF model"""
    try:
        success = training_service.delete_model(model_name)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Model not found: {model_name}")
        
        return {
            "status": "success",
            "message": f"Model {model_name} deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting model: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete model: {str(e)}")

# Anomaly detection endpoints
@router.post("/twif/detect")
async def detect_anomalies(request: AnomalyDetectionRequest):
    """
    Detect anomalies in database using TWIF model
    
    Args:
        request: Database path and model configuration
    
    Returns:
        Anomaly detection results
    """
    try:
        logger.info(f"Starting anomaly detection: {request.model_name}")
        
        results = anomaly_service.detect_anomalies(
            db_path=request.db_path,
            model_name=request.model_name,
            anomaly_threshold=request.anomaly_threshold
        )
        
        return {
            "status": "success",
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Anomaly detection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Detection failed: {str(e)}")

@router.post("/twif/analyze/table")
async def analyze_table_anomalies(request: TableAnalysisRequest):
    """
    Get detailed anomaly analysis for a specific table
    
    Args:
        request: Database path, table name, and model configuration
    
    Returns:
        Detailed table anomaly analysis
    """
    try:
        logger.info(f"Analyzing table anomalies: {request.table_name}")
        
        analysis = anomaly_service.get_table_anomaly_details(
            db_path=request.db_path,
            table_name=request.table_name,
            model_name=request.model_name
        )
        
        if analysis is None:
            raise HTTPException(status_code=404, detail=f"Table not found: {request.table_name}")
        
        return {
            "status": "success",
            "analysis": analysis
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Table analysis error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@router.post("/twif/trends")
async def get_anomaly_trends(request: AnomalyDetectionRequest, historical_data: Optional[List[Dict[str, Any]]] = None):
    """
    Analyze anomaly trends over time
    
    Args:
        request: Database path and model configuration
        historical_data: Optional historical anomaly results
    
    Returns:
        Trend analysis and recommendations
    """
    try:
        logger.info("Analyzing anomaly trends")
        
        trends = anomaly_service.get_anomaly_trends(
            db_path=request.db_path,
            model_name=request.model_name,
            historical_results=historical_data
        )
        
        return {
            "status": "success",
            "trends": trends
        }
        
    except Exception as e:
        logger.error(f"Trend analysis error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Trend analysis failed: {str(e)}")

# Health check endpoint
@router.get("/twif/health")
async def twif_health_check():
    """TWIF service health check"""
    try:
        models = training_service.list_models()
        
        return {
            "status": "healthy",
            "models_available": models["total_count"],
            "services": {
                "training": "operational",
                "anomaly_detection": "operational"
            }
        }
        
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        return {
            "status": "degraded",
            "error": str(e),
            "services": {
                "training": "error",
                "anomaly_detection": "error"
            }
        }

# Quick demo endpoint
@router.post("/twif/demo")
async def quick_demo(req: DemoRequest):
    """
    Quick demo: Train model and detect anomalies in one call
    
    Args:
        req: Demo request body
    
    Returns:
        Complete demo results
    """
    try:
        logger.info("Starting TWIF demo")
        requested = req.db_path
        resolved_path = _resolve_demo_db_path(requested)
        db_path = str(resolved_path)
        logger.info(f"TWIF demo db_path requested={requested} resolved={db_path}")
        
        # Step 1: Train model
        training_results = training_service.train_model(
            db_path=db_path,
            model_name="twif_demo",
            n_estimators=100,  # Faster for demo
            topology_weight=0.4,
            relationship_boost=1.5
        )
        
        # Step 2: Detect anomalies
        detection_results = anomaly_service.detect_anomalies(
            db_path=db_path,
            model_name="twif_demo"
        )
        
        # Step 3: Get top anomalous tables
        anomalous_tables = [
            table for table in detection_results['tables'] 
            if table['is_anomaly']
        ][:5]  # Top 5
        
        payload = _to_json_safe({
            "status": "success",
            "demo_results": {
                "training": training_results,
                "detection_summary": detection_results['summary'],
                "top_anomalous_tables": anomalous_tables,
                "insights": detection_results.get('insights', {})
            }
        })
        # Force conversion through stdlib json so FastAPI never sees numpy scalars.
        return JSONResponse(content=json.loads(json.dumps(payload)))
        
    except Exception as e:
        logger.error(f"Demo error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Demo failed: {str(e)}")
