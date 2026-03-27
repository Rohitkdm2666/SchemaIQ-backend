"""
TWIF Anomaly Detection Service
Handles anomaly detection using trained TWIF models
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

from models.twif_model import TopologyWeightedIsolationForest
from services.twif_training import TWIFTrainingService

logger = logging.getLogger(__name__)

class TWIFAnomalyService:
    """Service for database anomaly detection using TWIF models"""
    
    def __init__(self, models_dir: str = "models"):
        self.models_dir = Path(models_dir)
        self.training_service = TWIFTrainingService(models_dir)
        self.loaded_models = {}
        
    def load_model(self, model_name: str) -> Optional[TopologyWeightedIsolationForest]:
        """Load TWIF model by name"""
        if model_name in self.loaded_models:
            return self.loaded_models[model_name]
        
        model_path = self.models_dir / f"{model_name}.pkl"
        if not model_path.exists():
            logger.error(f"Model not found: {model_name}")
            return None
        
        try:
            twif = TopologyWeightedIsolationForest()
            twif.load_model(str(model_path))
            self.loaded_models[model_name] = twif
            logger.info(f"Loaded TWIF model: {model_name}")
            return twif
            
        except Exception as e:
            logger.error(f"Failed to load model {model_name}: {str(e)}")
            return None
    
    def detect_anomalies(self, 
                        db_path: str, 
                        model_name: str = "twif_default",
                        anomaly_threshold: float = None) -> Dict[str, Any]:
        """
        Detect anomalies in database using TWIF model
        
        Args:
            db_path: Path to SQLite database
            model_name: Name of trained model to use
            anomaly_threshold: Custom threshold for anomaly detection
            
        Returns:
            Anomaly detection results
        """
        logger.info(f"Starting anomaly detection with model: {model_name}")
        
        try:
            # Load model
            twif = self.load_model(model_name)
            if twif is None:
                raise ValueError(f"Model not found: {model_name}")
            
            # Run prediction
            results = twif.predict(db_path)
            
            # Apply custom threshold if provided
            if anomaly_threshold is not None:
                results = self._apply_custom_threshold(results, anomaly_threshold)
            
            # Add metadata
            results['detection_metadata'] = {
                'model_name': model_name,
                'database_path': db_path,
                'detection_timestamp': datetime.now().isoformat(),
                'anomaly_threshold': anomaly_threshold,
                'twif_version': '1.0'
            }
            
            # Generate insights
            results['insights'] = self._generate_insights(results)
            
            logger.info(f"Anomaly detection completed: {results['summary']['anomaly_count']} anomalies")
            return results
            
        except Exception as e:
            logger.error(f"Anomaly detection failed: {str(e)}")
            raise
    
    def _apply_custom_threshold(self, results: Dict[str, Any], threshold: float) -> Dict[str, Any]:
        """Apply custom anomaly threshold"""
        original_anomaly_count = results['summary']['anomaly_count']
        
        # Update anomaly status based on threshold
        new_anomaly_count = 0
        for table in results['tables']:
            table['is_anomaly'] = table['anomaly_score'] < threshold
            if table['is_anomaly']:
                new_anomaly_count += 1
        
        # Update summary
        results['summary']['anomaly_count'] = new_anomaly_count
        results['summary']['anomaly_percentage'] = (new_anomaly_count / len(results['tables'])) * 100
        results['summary']['threshold_applied'] = threshold
        
        logger.info(f"Threshold applied: {original_anomaly_count} → {new_anomaly_count} anomalies")
        return results
    
    def _generate_insights(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate insights from anomaly detection results"""
        insights = {
            'key_findings': [],
            'recommendations': [],
            'risk_assessment': 'low'
        }
        
        # Analyze anomaly patterns
        anomaly_tables = [t for t in results['tables'] if t['is_anomaly']]
        total_tables = len(results['tables'])
        anomaly_rate = len(anomaly_tables) / total_tables if total_tables > 0 else 0
        
        # Risk assessment
        if anomaly_rate > 0.5:
            insights['risk_assessment'] = 'high'
        elif anomaly_rate > 0.2:
            insights['risk_assessment'] = 'medium'
        
        # Key findings
        if anomaly_tables:
            insights['key_findings'].append(
                f"{len(anomaly_tables)} tables ({anomaly_rate*100:.1f}%) show anomalous patterns"
            )
            
            # Most severe anomalies
            severe_tables = [t for t in anomaly_tables if t['anomaly_level'] == 'severe']
            if severe_tables:
                insights['key_findings'].append(
                    f"{len(severe_tables)} tables have severe anomalies"
                )
            
            # Topology-related anomalies
            topology_anomalies = [t for t in anomaly_tables 
                                if t['topology_insights']['topology_factors']]
            if topology_anomalies:
                insights['key_findings'].append(
                    f"{len(topology_anomalies)} anomalies related to database topology"
                )
        
        # Recommendations
        if anomaly_rate > 0.3:
            insights['recommendations'].append(
                "High anomaly rate detected - consider comprehensive data quality review"
            )
        
        if severe_tables:
            insights['recommendations'].append(
                "Investigate severely anomalous tables immediately"
            )
        
        # Check for common patterns
        high_null_tables = [t for t in anomaly_tables 
                           if t['metrics'].get('avg_null_pct', 0) > 0.3]
        if len(high_null_tables) > 1:
            insights['recommendations'].append(
                "Multiple tables with high null rates - check data pipeline"
            )
        
        return insights
    
    def get_table_anomaly_details(self, 
                                 db_path: str, 
                                 table_name: str, 
                                 model_name: str = "twif_default") -> Optional[Dict[str, Any]]:
        """Get detailed anomaly analysis for a specific table"""
        try:
            results = self.detect_anomalies(db_path, model_name)
            
            # Find the table
            table_data = None
            for table in results['tables']:
                if table['table_name'] == table_name:
                    table_data = table
                    break
            
            if table_data is None:
                return None
            
            # Enhanced analysis
            detailed_analysis = {
                **table_data,
                'anomaly_breakdown': self._analyze_table_anomaly_breakdown(table_data),
                'comparison_with_peers': self._compare_with_similar_tables(table_data, results['tables']),
                'remediation_steps': self._suggest_remediation_steps(table_data)
            }
            
            return detailed_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing table {table_name}: {str(e)}")
            return None
    
    def _analyze_table_anomaly_breakdown(self, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze what specific metrics are causing the anomaly"""
        breakdown = {
            'primary_factors': [],
            'metric_contributions': {},
            'severity_drivers': []
        }
        
        metrics = table_data.get('metrics', {})
        anomaly_score = table_data.get('anomaly_score', 0)
        
        # Check each metric for unusual values
        metric_checks = {
            'avg_null_pct': ('High null percentage', 0.3),
            'max_null_pct': ('Extreme null values', 0.8),
            'row_count': ('Unusual row count', None),  # Will check relative to others
            'column_count': ('Unusual column count', None),
            'fk_to_col_ratio': ('Low foreign key ratio', 0.1),
            'topology_importance': ('High importance with anomalies', 0.7)
        }
        
        for metric, (description, threshold) in metric_checks.items():
            value = metrics.get(metric, 0)
            
            if threshold and value > threshold:
                breakdown['primary_factors'].append(f"{description}: {value:.3f}")
                breakdown['metric_contributions'][metric] = value
        
        # Severity drivers
        if anomaly_score < -0.5:
            breakdown['severity_drivers'].append('Very low anomaly score indicates severe deviation')
        
        if table_data.get('topology_insights', {}).get('topology_factors'):
            breakdown['severity_drivers'].append('Topology-related factors contributing to anomaly')
        
        return breakdown
    
    def _compare_with_similar_tables(self, 
                                   table_data: Dict[str, Any], 
                                   all_tables: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compare anomalous table with similar tables"""
        comparison = {
            'similar_tables': [],
            'outlier_metrics': [],
            'peer_analysis': 'No similar tables found'
        }
        
        current_metrics = table_data.get('metrics', {})
        current_table_name = table_data.get('table_name', '')
        
        # Find similar tables (based on row count and column count)
        similar_tables = []
        for table in all_tables:
            if table['table_name'] == current_table_name:
                continue
            
            metrics = table.get('metrics', {})
            
            # Similarity criteria
            row_diff = abs(current_metrics.get('row_count', 0) - metrics.get('row_count', 0))
            col_diff = abs(current_metrics.get('column_count', 0) - metrics.get('column_count', 0))
            
            if row_diff < current_metrics.get('row_count', 1) * 0.5 and col_diff <= 2:
                similar_tables.append(table)
        
        if similar_tables:
            comparison['similar_tables'] = [t['table_name'] for t in similar_tables[:5]]
            
            # Compare metrics
            for metric in ['avg_null_pct', 'fk_to_col_ratio', 'topology_importance']:
                current_value = current_metrics.get(metric, 0)
                peer_values = [t.get('metrics', {}).get(metric, 0) for t in similar_tables]
                
                if peer_values:
                    peer_avg = sum(peer_values) / len(peer_values)
                    deviation = abs(current_value - peer_avg)
                    
                    if deviation > 0.2:  # Significant deviation
                        comparison['outlier_metrics'].append({
                            'metric': metric,
                            'current_value': current_value,
                            'peer_average': peer_avg,
                            'deviation': deviation
                        })
            
            comparison['peer_analysis'] = f"Compared against {len(similar_tables)} similar tables"
        
        return comparison
    
    def _suggest_remediation_steps(self, table_data: Dict[str, Any]) -> List[str]:
        """Suggest remediation steps for anomalous table"""
        steps = []
        metrics = table_data.get('metrics', {})
        anomaly_level = table_data.get('anomaly_level', 'mild')
        
        # High null percentage
        if metrics.get('avg_null_pct', 0) > 0.3:
            steps.append("Investigate data pipeline for missing values")
            steps.append("Consider data validation rules at source")
        
        # Low foreign key ratio
        if metrics.get('fk_to_col_ratio', 0) < 0.1:
            steps.append("Review referential integrity constraints")
            steps.append("Consider adding missing foreign key relationships")
        
        # No primary key
        if not metrics.get('has_primary_key', 0):
            steps.append("Add primary key constraint for data integrity")
        
        # High importance table with issues
        if metrics.get('topology_importance', 0) > 0.7:
            steps.append("Priority: This table is critical to database topology")
            steps.append("Schedule immediate data quality review")
        
        # Severity-based recommendations
        if anomaly_level == 'severe':
            steps.append("URGENT: Immediate investigation required")
            steps.append("Consider temporarily quarantining affected data")
        elif anomaly_level == 'moderate':
            steps.append("Schedule review within next business cycle")
        
        return steps
    
    def get_anomaly_trends(self, 
                          db_path: str, 
                          model_name: str = "twif_default",
                          historical_results: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Analyze anomaly trends over time"""
        current_results = self.detect_anomalies(db_path, model_name)
        
        trends = {
            'current_snapshot': current_results['summary'],
            'trend_analysis': 'No historical data available',
            'trend_direction': 'stable',
            'recommendations': []
        }
        
        if historical_results:
            # Simple trend analysis
            recent_anomalies = [r['summary']['anomaly_count'] for r in historical_results[-5:]]
            current_count = current_results['summary']['anomaly_count']
            
            if len(recent_anomalies) > 1:
                avg_recent = sum(recent_anomalies) / len(recent_anomalies)
                
                if current_count > avg_recent * 1.2:
                    trends['trend_direction'] = 'increasing'
                    trends['recommendations'].append("Anomaly count increasing - monitor closely")
                elif current_count < avg_recent * 0.8:
                    trends['trend_direction'] = 'decreasing'
                    trends['recommendations'].append("Anomaly count decreasing - improvements working")
                
                trends['trend_analysis'] = f"Current: {current_count}, Recent average: {avg_recent:.1f}"
        
        return trends
