"""
TWIF Training Service
Handles training pipeline for Topology-Weighted Isolation Forest model
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional
import sqlite3
import json
from datetime import datetime

from models.twif_model import TopologyWeightedIsolationForest, train_twif_model

logger = logging.getLogger(__name__)

class TWIFTrainingService:
    """Service for training and managing TWIF models"""
    
    def __init__(self, models_dir: str = "models"):
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(exist_ok=True)
        
    def train_model(self, 
                   db_path: str, 
                   model_name: str = "twif_default",
                   **training_params) -> Dict[str, Any]:
        """
        Train TWIF model on database
        
        Args:
            db_path: Path to SQLite database
            model_name: Name for the trained model
            **training_params: Additional parameters for TWIF
            
        Returns:
            Training results and model info
        """
        logger.info(f"Starting TWIF training for {model_name}")
        
        try:
            # Validate database
            if not Path(db_path).exists():
                raise FileNotFoundError(f"Database not found: {db_path}")
            
            # Create model with custom parameters
            twif_params = {
                'n_estimators': training_params.get('n_estimators', 150),
                'topology_weight': training_params.get('topology_weight', 0.4),
                'relationship_boost': training_params.get('relationship_boost', 1.8),
                'contamination': training_params.get('contamination', 'auto'),
                'random_state': training_params.get('random_state', 42)
            }
            
            twif = TopologyWeightedIsolationForest(**twif_params)
            twif.fit(db_path)
            
            # Save model
            model_path = self.models_dir / f"{model_name}.pkl"
            twif.save_model(str(model_path))
            
            # Get training summary
            training_results = self._create_training_summary(twif, db_path, model_name)
            
            logger.info(f"TWIF training completed: {model_name}")
            return training_results
            
        except Exception as e:
            logger.error(f"TWIF training failed: {str(e)}")
            raise
    
    def _create_training_summary(self, 
                                twif: TopologyWeightedIsolationForest, 
                                db_path: str, 
                                model_name: str) -> Dict[str, Any]:
        """Create training summary report"""
        
        # Get basic database info
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        table_count = cursor.fetchone()[0]
        
        # Get total row count
        cursor.execute("""
            SELECT SUM(sql) FROM (
                SELECT sql FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            )
        """)
        
        conn.close()
        
        summary = {
            'model_name': model_name,
            'training_timestamp': datetime.now().isoformat(),
            'database_path': db_path,
            'database_info': {
                'table_count': table_count
            },
            'model_parameters': {
                'n_estimators': twif.n_estimators,
                'topology_weight': twif.topology_weight,
                'relationship_boost': twif.relationship_boost,
                'contamination': twif.contamination,
                'random_state': twif.random_state
            },
            'topology_info': {
                'tables_analyzed': len(twif.topology_weights.table_importance) if twif.topology_weights else 0,
                'relationships_found': len(twif.topology_weights.relationship_strength) if twif.topology_weights else 0,
                'feature_count': len(twif.feature_names),
                'topology_features': len(twif.topology_features)
            },
            'feature_importance': dict(list(twif.feature_topology_scores.items())[:10]),  # Top 10
            'status': 'completed'
        }
        
        return summary
    
    def list_models(self) -> Dict[str, Any]:
        """List all trained TWIF models"""
        models = []
        
        for model_file in self.models_dir.glob("*.pkl"):
            try:
                # Load model metadata
                twif = TopologyWeightedIsolationForest()
                twif.load_model(str(model_file))
                
                model_info = {
                    'name': model_file.stem,
                    'path': str(model_file),
                    'size_mb': model_file.stat().st_size / (1024 * 1024),
                    'parameters': {
                        'n_estimators': twif.n_estimators,
                        'topology_weight': twif.topology_weight,
                        'relationship_boost': twif.relationship_boost
                    },
                    'feature_count': len(twif.feature_names) if twif.feature_names else 0
                }
                models.append(model_info)
                
            except Exception as e:
                logger.warning(f"Could not load model {model_file}: {str(e)}")
        
        return {
            'models': models,
            'total_count': len(models)
        }
    
    def delete_model(self, model_name: str) -> bool:
        """Delete a trained model"""
        model_path = self.models_dir / f"{model_name}.pkl"
        
        if model_path.exists():
            model_path.unlink()
            logger.info(f"Deleted model: {model_name}")
            return True
        
        return False
    
    def get_model_info(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific model"""
        model_path = self.models_dir / f"{model_name}.pkl"
        
        if not model_path.exists():
            return None
        
        try:
            twif = TopologyWeightedIsolationForest()
            twif.load_model(str(model_path))
            
            return {
                'name': model_name,
                'path': str(model_path),
                'parameters': {
                    'n_estimators': twif.n_estimators,
                    'topology_weight': twif.topology_weight,
                    'relationship_boost': twif.relationship_boost,
                    'contamination': twif.contamination,
                    'random_state': twif.random_state
                },
                'topology_info': {
                    'tables_analyzed': len(twif.topology_weights.table_importance) if twif.topology_weights else 0,
                    'relationships_found': len(twif.topology_weights.relationship_strength) if twif.topology_weights else 0,
                    'feature_count': len(twif.feature_names) if twif.feature_names else 0,
                    'topology_features': len(twif.topology_features) if twif.topology_features else 0
                },
                'feature_importance': twif.feature_topology_scores
            }
            
        except Exception as e:
            logger.error(f"Error loading model {model_name}: {str(e)}")
            return None
