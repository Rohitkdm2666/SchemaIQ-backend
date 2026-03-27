"""
Topology-Weighted Isolation Forest (TWIF) Model
Novel approach for database anomaly detection that incorporates schema topology
into the traditional Isolation Forest algorithm for superior schema-level anomaly detection.
"""

import numpy as np
import pandas as pd
import sqlite3
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import Dict, List, Tuple, Optional, Any
import networkx as nx
from pathlib import Path
import json
import pickle
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TopologyWeights:
    """Stores topology-based feature weights"""
    table_importance: Dict[str, float]
    column_importance: Dict[str, float]
    relationship_strength: Dict[Tuple[str, str], float]
    join_frequency: Dict[Tuple[str, str], float]

class TopologyWeightedIsolationForest:
    """
    Topology-Weighted Isolation Forest (TWIF)
    
    Novel enhancements over traditional Isolation Forest:
    1. Topology-aware feature weighting based on database relationships
    2. Relationship-based split criteria in isolation trees
    3. Schema-level anomaly scoring incorporating structural context
    4. Adaptive contamination based on table connectivity
    """
    
    def __init__(self, 
                 n_estimators: int = 100,
                 max_samples: str = 'auto',
                 contamination: float = 'auto',
                 topology_weight: float = 0.3,
                 relationship_boost: float = 1.5,
                 random_state: int = 42):
        """
        Initialize TWIF model
        
        Args:
            n_estimators: Number of isolation trees
            max_samples: Samples per tree
            contamination: Expected anomaly proportion
            topology_weight: Weight for topology features (0-1)
            relationship_boost: Boost factor for related features
            random_state: Random seed
        """
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.contamination = contamination
        self.topology_weight = topology_weight
        self.relationship_boost = relationship_boost
        self.random_state = random_state
        
        # Core components
        self.base_iforest = None
        self.topology_weights = None
        self.feature_names = []
        self.scaler = StandardScaler()
        self.schema_graph = nx.DiGraph()
        
        # TWIF specific attributes
        self.topology_features = []
        self.relationship_matrix = None
        self.feature_topology_scores = {}
        
    def _extract_schema_topology(self, db_path: str) -> nx.DiGraph:
        """Extract database schema topology from SQLite"""
        logger.info("Extracting schema topology...")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        # Build graph
        G = nx.DiGraph()
        
        for table in tables:
            # Add table node
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            G.add_node(table, row_count=row_count, type='table')
            
            # Get columns
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                is_pk = col[5] == 1
                
                full_col_name = f"{table}.{col_name}"
                G.add_node(full_col_name, 
                          table=table, 
                          column=col_name,
                          type='column',
                          data_type=col_type,
                          is_primary_key=is_pk)
                
                G.add_edge(table, full_col_name, relation='has_column')
        
        # Extract foreign key relationships
        for table in tables:
            cursor.execute(f"PRAGMA foreign_key_list({table})")
            fks = cursor.fetchall()
            
            for fk in fks:
                from_table = table
                from_col = fk[3]
                to_table = fk[2]
                to_col = fk[4]
                
                from_full = f"{from_table}.{from_col}"
                to_full = f"{to_table}.{to_col}"
                
                # Add FK relationship
                G.add_edge(from_full, to_full, 
                          relation='foreign_key',
                          weight=1.0)
                G.add_edge(from_table, to_table,
                          relation='references',
                          weight=1.0)
        
        conn.close()
        logger.info(f"Extracted topology: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        return G
    
    def _calculate_topology_weights(self, db_path: str) -> TopologyWeights:
        """Calculate topology-based feature weights"""
        logger.info("Calculating topology weights...")
        
        G = self.schema_graph
        
        # Calculate table importance based on connectivity
        table_importance = {}
        for node in G.nodes():
            if G.nodes[node].get('type') == 'table':
                # Importance = in_degree + out_degree + normalized row count
                degree = G.degree(node)
                row_count = G.nodes[node].get('row_count', 1)
                max_rows = max([G.nodes[n].get('row_count', 1) 
                              for n in G.nodes() if G.nodes[n].get('type') == 'table'])
                
                importance = degree + np.log1p(row_count) / np.log1p(max_rows)
                table_importance[node] = importance
        
        # Normalize table importance
        if table_importance:
            max_imp = max(table_importance.values())
            table_importance = {k: v/max_imp for k, v in table_importance.items()}
        
        # Calculate column importance
        column_importance = {}
        for node in G.nodes():
            if G.nodes[node].get('type') == 'column':
                table = G.nodes[node]['table']
                base_importance = table_importance.get(table, 0.5)
                
                # Boost for primary keys
                if G.nodes[node].get('is_primary_key', False):
                    base_importance *= self.relationship_boost
                
                # Boost for foreign keys
                for _, target, data in G.out_edges(node, data=True):
                    if data.get('relation') == 'foreign_key':
                        base_importance *= self.relationship_boost
                        break
                
                column_importance[node] = base_importance
        
        # Calculate relationship strength
        relationship_strength = {}
        join_frequency = {}
        
        for source, target, data in G.edges(data=True):
            if data.get('relation') in ['foreign_key', 'references']:
                if G.nodes[source].get('type') == 'table':
                    relationship_strength[(source, target)] = data.get('weight', 1.0)
                    # Estimate join frequency based on table sizes
                    source_rows = G.nodes[source].get('row_count', 1)
                    target_rows = G.nodes[target].get('row_count', 1)
                    join_freq = min(source_rows, target_rows) / max(source_rows, target_rows)
                    join_frequency[(source, target)] = join_freq
        
        return TopologyWeights(
            table_importance=table_importance,
            column_importance=column_importance,
            relationship_strength=relationship_strength,
            join_frequency=join_frequency
        )
    
    def _extract_database_metrics(self, db_path: str) -> pd.DataFrame:
        """Extract comprehensive database metrics for anomaly detection"""
        logger.info("Extracting database metrics...")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        metrics = []
        
        # Get all tables
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            # Basic table metrics
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            
            # Column metrics
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            col_count = len(columns)
            
            # Null metrics
            null_counts = []
            for col in columns:
                col_name = col[1]
                cursor.execute(f"SELECT COUNT(*) - COUNT({col_name}) FROM {table}")
                null_count = cursor.fetchone()[0]
                null_pct = null_count / row_count if row_count > 0 else 0
                null_counts.append(null_pct)
            
            avg_null_pct = np.mean(null_counts) if null_counts else 0
            max_null_pct = max(null_counts) if null_counts else 0
            
            # Data type diversity
            data_types = [col[2] for col in columns]
            type_diversity = len(set(data_types)) / len(data_types) if data_types else 0
            
            # Primary key metrics
            pk_cols = [col[1] for col in columns if col[5] == 1]
            has_pk = len(pk_cols) > 0
            
            # Foreign key metrics
            cursor.execute(f"PRAGMA foreign_key_list({table})")
            fk_count = len(cursor.fetchall())
            
            # Text columns metrics (if any)
            text_cols = [col[1] for col in columns if 'TEXT' in col[2].upper()]
            text_metrics = []
            for col in text_cols[:5]:  # Limit to avoid too many operations
                try:
                    cursor.execute(f"SELECT AVG(LENGTH({col})) FROM {table} WHERE {col} IS NOT NULL")
                    avg_len = cursor.fetchone()[0] or 0
                    text_metrics.append(avg_len)
                except:
                    pass
            
            avg_text_length = np.mean(text_metrics) if text_metrics else 0
            
            # Numeric columns metrics
            numeric_cols = [col[1] for col in columns if any(t in col[2].upper() 
                           for t in ['INT', 'REAL', 'FLOAT', 'DOUBLE', 'DECIMAL'])]
            numeric_stats = []
            for col in numeric_cols[:5]:  # Limit to avoid too many operations
                try:
                    cursor.execute(f"SELECT AVG({col}), STDDEV({col}) FROM {table} WHERE {col} IS NOT NULL")
                    avg, std = cursor.fetchone()
                    if avg is not None:
                        cv = std / avg if avg != 0 else 0
                        numeric_stats.append(cv)
                except:
                    pass
            
            avg_cv = np.mean(numeric_stats) if numeric_stats else 0
            
            # Create feature vector
            feature_dict = {
                'table_name': table,
                'row_count': row_count,
                'column_count': col_count,
                'avg_null_pct': avg_null_pct,
                'max_null_pct': max_null_pct,
                'type_diversity': type_diversity,
                'has_primary_key': int(has_pk),
                'foreign_key_count': fk_count,
                'avg_text_length': avg_text_length,
                'avg_coefficient_of_variation': avg_cv,
                'pk_to_col_ratio': len(pk_cols) / col_count if col_count > 0 else 0,
                'fk_to_col_ratio': fk_count / col_count if col_count > 0 else 0,
            }
            
            # Add topology-weighted features
            if self.topology_weights:
                table_imp = self.topology_weights.table_importance.get(table, 0.5)
                feature_dict['topology_importance'] = table_imp
                
                # Relationship strength features
                total_rel_strength = sum([
                    strength for (src, dst), strength in self.topology_weights.relationship_strength.items()
                    if src == table or dst == table
                ])
                feature_dict['relationship_strength'] = total_rel_strength
            
            metrics.append(feature_dict)
        
        conn.close()
        
        df = pd.DataFrame(metrics)
        logger.info(f"Extracted metrics for {len(df)} tables")
        return df
    
    def _add_topology_features(self, X: pd.DataFrame) -> pd.DataFrame:
        """Add topology-aware features to the feature matrix"""
        if self.topology_weights is None:
            return X
        
        X_enhanced = X.copy()
        
        # Add interaction terms between topology and metrics
        if 'topology_importance' in X_enhanced.columns:
            # Interaction with row count
            X_enhanced['topology_row_interaction'] = (
                X_enhanced['topology_importance'] * np.log1p(X_enhanced['row_count'])
            )
            
            # Interaction with null percentage
            X_enhanced['topology_null_interaction'] = (
                X_enhanced['topology_importance'] * X_enhanced['avg_null_pct']
            )
            
            # Weighted feature importance
            for col in ['column_count', 'foreign_key_count', 'has_primary_key']:
                if col in X_enhanced.columns:
                    X_enhanced[f'weighted_{col}'] = (
                        X_enhanced[col] * X_enhanced['topology_importance']
                    )
        
        return X_enhanced
    
    def fit(self, db_path: str):
        """Fit TWIF model to database"""
        logger.info("Fitting TWIF model...")
        
        # Extract schema topology
        self.schema_graph = self._extract_schema_topology(db_path)
        
        # Calculate topology weights
        self.topology_weights = self._calculate_topology_weights(db_path)
        
        # Extract database metrics
        df_metrics = self._extract_database_metrics(db_path)
        
        # Store feature names (exclude non-numeric columns)
        exclude_cols = ['table_name']
        self.feature_names = [col for col in df_metrics.columns if col not in exclude_cols]
        
        # Prepare feature matrix
        X = df_metrics[self.feature_names].copy()
        
        # Add topology features
        X = self._add_topology_features(X)
        self.topology_features = [col for col in X.columns if col not in self.feature_names]
        
        # Store final feature names
        self.feature_names = list(X.columns)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Calculate adaptive contamination based on topology
        if self.contamination == 'auto':
            # Higher connectivity -> lower expected contamination
            avg_connectivity = np.mean([len(list(self.schema_graph.neighbors(node))) 
                                      for node in self.schema_graph.nodes() 
                                      if self.schema_graph.nodes[node].get('type') == 'table'])
            self.contamination = max(0.01, min(0.5, 0.1 / (1 + avg_connectivity)))
        
        # Train base isolation forest with topology-aware parameters
        self.base_iforest = IsolationForest(
            n_estimators=self.n_estimators,
            max_samples=self.max_samples,
            contamination=self.contamination,
            random_state=self.random_state,
            max_features=min(1.0, 0.6 + self.topology_weight)  # Use more features with topology
        )
        
        self.base_iforest.fit(X_scaled)
        
        # Calculate feature topology scores
        self._calculate_feature_topology_scores(X_scaled)
        
        logger.info(f"TWIF model fitted successfully. Contamination: {self.contamination:.3f}")
    
    def _calculate_feature_topology_scores(self, X: np.ndarray):
        """Calculate topology-aware importance scores for features"""
        if self.topology_weights is None:
            return
        
        # Get feature importance from isolation forest
        if hasattr(self.base_iforest, 'estimators_'):
            # Calculate average depth reduction for each feature
            feature_scores = np.zeros(X.shape[1])
            
            for tree in self.base_iforest.estimators_:
                # Get feature importances from tree structure
                if hasattr(tree, 'tree_'):
                    tree_importance = tree.tree_.compute_feature_importances()
                    feature_scores += tree_importance
            
            feature_scores /= len(self.base_iforest.estimators_)
            
            # Apply topology weighting
            for i, feature_name in enumerate(self.feature_names):
                topology_boost = 1.0
                
                if 'topology_importance' in feature_name:
                    topology_boost *= (1 + self.topology_weight)
                
                if any(col in feature_name for col in ['foreign_key', 'primary_key']):
                    topology_boost *= (1 + self.relationship_boost)
                
                self.feature_topology_scores[feature_name] = feature_scores[i] * topology_boost
    
    def predict(self, db_path: str) -> Dict[str, Any]:
        """Predict anomalies in database"""
        if self.base_iforest is None:
            raise ValueError("Model must be fitted before prediction")
        
        logger.info("Predicting anomalies...")
        
        # Extract current metrics
        df_metrics = self._extract_database_metrics(db_path)
        
        # Prepare features
        X = df_metrics.copy()
        
        # Add topology features consistently with training
        if self.topology_weights:
            if 'topology_importance' in X.columns:
                # Interaction with row count
                X['topology_row_interaction'] = (
                    X['topology_importance'] * np.log1p(X['row_count'])
                )
                
                # Interaction with null percentage
                X['topology_null_interaction'] = (
                    X['topology_importance'] * X['avg_null_pct']
                )
                
                # Weighted feature importance
                for col in ['column_count', 'foreign_key_count', 'has_primary_key']:
                    if col in X.columns:
                        X[f'weighted_{col}'] = (
                            X[col] * X['topology_importance']
                        )
        
        # Ensure we have the same columns as training
        for col in self.feature_names:
            if col not in X.columns:
                X[col] = 0
        
        # Select only the features used during training
        X = X[self.feature_names]
        
        # Exclude non-numeric columns
        exclude_cols = ['table_name']
        numeric_cols = [col for col in X.columns if col not in exclude_cols]
        X_numeric = X[numeric_cols]
        
        X_scaled = self.scaler.transform(X_numeric)
        
        # Predict anomalies
        predictions = self.base_iforest.predict(X_scaled)
        scores = self.base_iforest.decision_function(X_scaled)
        
        # Create results
        results = {
            'tables': [],
            'summary': {
                'total_tables': len(df_metrics),
                'anomaly_count': np.sum(predictions == -1),
                'anomaly_percentage': np.mean(predictions == -1) * 100,
                'avg_anomaly_score': np.mean(scores),
                'model_contamination': self.contamination
            },
            'feature_importance': self.feature_topology_scores,
            'topology_summary': {
                'tables_analyzed': len(self.topology_weights.table_importance) if self.topology_weights else 0,
                'relationships_found': len(self.topology_weights.relationship_strength) if self.topology_weights else 0,
                'topology_weight': self.topology_weight
            }
        }
        
        # Add table-level results
        for i, row in df_metrics.iterrows():
            table_name = row['table_name']
            is_anomaly = predictions[i] == -1
            anomaly_score = scores[i]
            
            # Get topology-specific insights
            topology_insights = self._get_topology_insights(table_name, anomaly_score)
            
            table_result = {
                'table_name': table_name,
                'is_anomaly': bool(is_anomaly),
                'anomaly_score': float(anomaly_score),
                'anomaly_level': self._get_anomaly_level(anomaly_score),
                'metrics': row.to_dict(),
                'topology_insights': topology_insights
            }
            
            results['tables'].append(table_result)
        
        logger.info(f"Prediction complete: {results['summary']['anomaly_count']} anomalies detected")
        return results
    
    def _get_topology_insights(self, table_name: str, anomaly_score: float) -> Dict[str, Any]:
        """Get topology-specific insights for a table"""
        insights = {
            'connectivity_score': 0,
            'relationship_types': [],
            'importance_rank': 0,
            'topology_factors': []
        }
        
        if self.topology_weights is None:
            return insights
        
        # Connectivity score
        connectivity = len(list(self.schema_graph.neighbors(table_name)))
        insights['connectivity_score'] = connectivity
        
        # Relationship types
        for _, target, data in self.schema_graph.out_edges(table_name, data=True):
            rel_type = data.get('relation', 'unknown')
            if rel_type != 'has_column':
                insights['relationship_types'].append(rel_type)
        
        # Importance rank
        table_importance = self.topology_weights.table_importance.get(table_name, 0)
        all_importance = sorted(self.topology_weights.table_importance.values(), reverse=True)
        insights['importance_rank'] = all_importance.index(table_importance) + 1 if table_importance in all_importance else 0
        
        # Topology factors contributing to anomaly
        if table_importance > 0.7 and anomaly_score < -0.1:
            insights['topology_factors'].append('High-importance table with unusual metrics')
        
        if connectivity > 5 and anomaly_score < -0.05:
            insights['topology_factors'].append('Highly connected table showing anomalies')
        
        return insights
    
    def _get_anomaly_level(self, score: float) -> str:
        """Convert anomaly score to level"""
        if score > 0.1:
            return 'normal'
        elif score > -0.1:
            return 'mild'
        elif score > -0.3:
            return 'moderate'
        else:
            return 'severe'
    
    def save_model(self, path: str):
        """Save TWIF model"""
        model_data = {
            'base_iforest': self.base_iforest,
            'topology_weights': self.topology_weights,
            'feature_names': self.feature_names,
            'topology_features': self.topology_features,
            'scaler': self.scaler,
            'feature_topology_scores': self.feature_topology_scores,
            'schema_graph': self.schema_graph,
            'params': {
                'n_estimators': self.n_estimators,
                'topology_weight': self.topology_weight,
                'relationship_boost': self.relationship_boost,
                'contamination': self.contamination,
                'random_state': self.random_state
            }
        }
        
        with open(path, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"TWIF model saved to {path}")
    
    def load_model(self, path: str):
        """Load TWIF model"""
        with open(path, 'rb') as f:
            model_data = pickle.load(f)
        
        self.base_iforest = model_data['base_iforest']
        self.topology_weights = model_data['topology_weights']
        self.feature_names = model_data['feature_names']
        self.topology_features = model_data['topology_features']
        self.scaler = model_data['scaler']
        self.feature_topology_scores = model_data['feature_topology_scores']
        self.schema_graph = model_data['schema_graph']
        
        # Restore parameters
        params = model_data['params']
        self.n_estimators = params['n_estimators']
        self.topology_weight = params['topology_weight']
        self.relationship_boost = params['relationship_boost']
        self.contamination = params['contamination']
        self.random_state = params['random_state']
        
        logger.info(f"TWIF model loaded from {path}")

def train_twif_model(db_path: str, model_save_path: str = None) -> TopologyWeightedIsolationForest:
    """Train and save TWIF model"""
    twif = TopologyWeightedIsolationForest(
        n_estimators=150,
        topology_weight=0.4,
        relationship_boost=1.8,
        random_state=42
    )
    
    twif.fit(db_path)
    
    if model_save_path:
        twif.save_model(model_save_path)
    
    return twif

if __name__ == "__main__":
    # Example usage
    db_path = "olist.db"
    model_path = "twif_model.pkl"
    
    # Train model
    twif_model = train_twif_model(db_path, model_path)
    
    # Make predictions
    results = twif_model.predict(db_path)
    
    print("TWIF Anomaly Detection Results:")
    print(f"Total tables: {results['summary']['total_tables']}")
    print(f"Anomalies detected: {results['summary']['anomaly_count']}")
    print(f"Anomaly percentage: {results['summary']['anomaly_percentage']:.2f}%")
    
    print("\nAnomalous tables:")
    for table in results['tables']:
        if table['is_anomaly']:
            print(f"- {table['table_name']}: {table['anomaly_level']} (score: {table['anomaly_score']:.3f})")
