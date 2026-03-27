"""
TWIF Model Training Script
Trains and tests the Topology-Weighted Isolation Forest model on olist.db
"""

import sys
import os
from pathlib import Path
import json
import logging

# Add the Schema Intelligence Engine to path
sys.path.append(str(Path(__file__).parent))

from models.twif_model import train_twif_model, TopologyWeightedIsolationForest
from services.twif_training import TWIFTrainingService
from services.twif_anomaly import TWIFAnomalyService

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main training and testing script"""
    
    # Paths
    db_path = Path(__file__).parent.parent / "olist.db"
    models_dir = Path(__file__).parent / "models"
    
    logger.info("=== TWIF Model Training & Testing ===")
    logger.info(f"Database: {db_path}")
    logger.info(f"Models directory: {models_dir}")
    
    # Check if database exists
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.info("Please ensure olist.db exists in the backend directory")
        return
    
    try:
        # Initialize services
        training_service = TWIFTrainingService()
        anomaly_service = TWIFAnomalyService()
        
        # Step 1: Train TWIF model
        logger.info("\n🚀 Step 1: Training TWIF model...")
        
        training_results = training_service.train_model(
            db_path=str(db_path),
            model_name="twif_olist",
            n_estimators=150,
            topology_weight=0.4,
            relationship_boost=1.8,
            contamination="auto",
            random_state=42
        )
        
        logger.info("✅ Training completed successfully!")
        logger.info(f"Model: {training_results['model_name']}")
        logger.info(f"Tables analyzed: {training_results['topology_info']['tables_analyzed']}")
        logger.info(f"Relationships found: {training_results['topology_info']['relationships_found']}")
        logger.info(f"Features extracted: {training_results['topology_info']['feature_count']}")
        
        # Step 2: Run anomaly detection
        logger.info("\n🔍 Step 2: Running anomaly detection...")
        
        detection_results = anomaly_service.detect_anomalies(
            db_path=str(db_path),
            model_name="twif_olist"
        )
        
        # Summary results
        summary = detection_results['summary']
        logger.info("✅ Anomaly detection completed!")
        logger.info(f"Total tables: {summary['total_tables']}")
        logger.info(f"Anomalies detected: {summary['anomaly_count']}")
        logger.info(f"Anomaly percentage: {summary['anomaly_percentage']:.2f}%")
        
        # Step 3: Show anomalous tables
        anomalous_tables = [t for t in detection_results['tables'] if t['is_anomaly']]
        
        if anomalous_tables:
            logger.info(f"\n🚨 Anomalous Tables ({len(anomalous_tables)}):")
            for table in sorted(anomalous_tables, key=lambda x: x['anomaly_score']):
                logger.info(f"  • {table['table_name']}: {table['anomaly_level']} (score: {table['anomaly_score']:.3f})")
                
                # Show key metrics
                metrics = table['metrics']
                logger.info(f"    - Rows: {metrics.get('row_count', 'N/A')}")
                logger.info(f"    - Null %: {metrics.get('avg_null_pct', 0)*100:.1f}%")
                logger.info(f"    - FK ratio: {metrics.get('fk_to_col_ratio', 0):.3f}")
                logger.info(f"    - Topology importance: {metrics.get('topology_importance', 0):.3f}")
        else:
            logger.info("\n✅ No anomalies detected - database appears healthy!")
        
        # Step 4: Show insights
        insights = detection_results.get('insights', {})
        if insights.get('key_findings'):
            logger.info(f"\n💡 Key Findings:")
            for finding in insights['key_findings']:
                logger.info(f"  • {finding}")
        
        if insights.get('recommendations'):
            logger.info(f"\n📋 Recommendations:")
            for rec in insights['recommendations']:
                logger.info(f"  • {rec}")
        
        # Step 5: Detailed analysis for most anomalous table
        if anomalous_tables:
            most_anomalous = min(anomalous_tables, key=lambda x: x['anomaly_score'])
            logger.info(f"\n🔬 Detailed Analysis: {most_anomalous['table_name']}")
            
            detailed = anomaly_service.get_table_anomaly_details(
                db_path=str(db_path),
                table_name=most_anomalous['table_name'],
                model_name="twif_olist"
            )
            
            if detailed:
                logger.info(f"Anomaly breakdown: {detailed.get('anomaly_breakdown', {})}")
                
                remediation = detailed.get('remediation_steps', [])
                if remediation:
                    logger.info("Remediation steps:")
                    for step in remediation:
                        logger.info(f"  • {step}")
        
        # Step 6: Save detailed results
        results_file = models_dir / "twif_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'training': training_results,
                'detection': detection_results,
                'timestamp': str(Path(__file__).stat().st_mtime)
            }, f, indent=2, default=str)
        
        logger.info(f"\n💾 Detailed results saved to: {results_file}")
        logger.info("\n🎉 TWIF model training and testing completed successfully!")
        
        # API Usage Instructions
        logger.info("\n📡 API Usage Instructions:")
        logger.info("Start the Schema Intelligence Engine:")
        logger.info(f"  cd \"{Path(__file__).parent}\"")
        logger.info("  python -m uvicorn main:app --reload --port 8001")
        logger.info("")
        logger.info("API Endpoints:")
        logger.info("  POST /twif/train - Train new model")
        logger.info("  POST /twif/detect - Detect anomalies")
        logger.info("  GET /twif/models - List trained models")
        logger.info("  POST /twif/demo - Quick demo")
        logger.info("")
        logger.info("Quick demo call:")
        logger.info("  POST http://localhost:8001/twif/demo")
        logger.info('  Body: {"db_path": "olist.db"}')
        
    except Exception as e:
        logger.error(f"❌ Error during training/testing: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
