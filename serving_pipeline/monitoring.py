import pandas as pd
from typing import Dict, Any, Optional
import os
import logging
import json

logger = logging.getLogger(__name__)


def generate_drift_report(
    current_df: pd.DataFrame, 
    reference_df: pd.DataFrame, 
    output_path: Optional[str] = None,
    include_classification: bool = False
) -> Dict[str, Any]:
    """
    Generate drift report and return metrics as JSON.
    
    Args:
        current_df: Current production data
        reference_df: Reference/baseline data
        output_path: Optional path to save HTML report
        include_classification: Whether to include classification metrics (requires target/prediction columns)
        
    Returns:
        Dictionary with drift metrics
    """
    # Lazy import 
    try:
        from evidently.report import Report
        from evidently.metric_preset import DataDriftPreset, ClassificationPreset
        from evidently import ColumnMapping
    except ImportError as e:
        logger.error(f"Failed to import Evidently AI: {e}")
        raise ImportError(
            "Evidently AI is not installed or incompatible. "
            "Please install: pip install evidently"
        ) from e
    
    logger.info(f"Generating drift report - Reference: {reference_df.shape}, Current: {current_df.shape}")
    logger.info(f"Include classification: {include_classification}")
    
    # Build metrics list - always include data drift
    metrics_list = [DataDriftPreset()]
    
    # Only include classification if requested and columns exist
    if include_classification:
        has_target = 'target' in reference_df.columns and 'target' in current_df.columns
        has_prediction = 'prediction' in reference_df.columns and 'prediction' in current_df.columns
        
        if has_target and has_prediction:
            metrics_list.append(ClassificationPreset())
            logger.info("Classification metrics added")
        else:
            logger.warning(f"Classification metrics requested but columns not found. "
                         f"Has target: {has_target}, Has prediction: {has_prediction}")
    
    # Define column mapping for better drift detection
    numerical_features = []
    categorical_features = []
    
    for col in reference_df.columns:
        if col in ['target', 'prediction', 'timestamp']:
            continue
        if reference_df[col].dtype in ['int64', 'float64']:
            numerical_features.append(col)
        else:
            categorical_features.append(col)
    
    column_mapping = ColumnMapping(
        target='target' if 'target' in reference_df.columns else None,
        prediction='prediction' if 'prediction' in reference_df.columns else None,
        numerical_features=numerical_features if numerical_features else None,
        categorical_features=categorical_features if categorical_features else None
    )
    
    logger.info(f"Column mapping - Numerical: {numerical_features}, Categorical: {categorical_features}")
    
    # Create report
    report = Report(metrics=metrics_list)
    
    try:
        report.run(
            reference_data=reference_df, 
            current_data=current_df,
            column_mapping=column_mapping
        )
        logger.info("Report execution completed")
    except Exception as e:
        logger.error(f"Error running report: {e}", exc_info=True)
        raise
    
    # Save HTML report if path provided
    if output_path:
        try:
            os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
            report.save_html(output_path)
            logger.info(f"HTML report saved to: {output_path}")
        except Exception as e:
            logger.error(f"Error saving HTML report: {e}", exc_info=True)
    
    # Extract metrics as dictionary
    metrics_dict = {}
    
    # Get report results
    try:
        report_dict = report.as_dict()
        
        # DEBUG: Log full structure (only first time or if debug enabled)
        if logger.level <= logging.DEBUG:
            logger.debug("="*50)
            logger.debug("FULL REPORT STRUCTURE:")
            logger.debug(json.dumps(report_dict, indent=2, default=str)[:2000])  # Limit to 2000 chars
            logger.debug("="*50)
        
        # Log top-level keys
        logger.info(f"Report dict keys: {list(report_dict.keys())}")
        
        # Extract data drift metrics
        if 'metrics' in report_dict:
            logger.info(f"Found {len(report_dict['metrics'])} metrics")
            
            for idx, metric in enumerate(report_dict['metrics']):
                metric_type = str(metric.get('metric', ''))
                logger.info(f"Processing metric {idx}: {metric_type}")
                logger.info(f"  Available keys: {list(metric.keys())}")
                
                # Try both 'metric_result' (0.4.x) and 'result' (newer versions)
                result = metric.get('metric_result', metric.get('result', {}))
                
                if not result:
                    logger.warning(f"  No result found for metric: {metric_type}")
                    continue
                
                logger.info(f"  Result keys: {list(result.keys())}")
                
                # ===== DATA DRIFT METRICS =====
                if 'DataDrift' in metric_type or 'data_drift' in metric_type.lower():
                    logger.info("  Processing DataDrift metric")
                    
                    # Overall dataset drift
                    if 'dataset_drift' in result:
                        metrics_dict['dataset_drift'] = result['dataset_drift']
                        logger.info(f"   dataset_drift: {result['dataset_drift']}")
                    else:
                        logger.warning("   'dataset_drift' not found in result")
                    
                    # Drift share (alternative to dataset_drift in some versions)
                    if 'drift_share' in result:
                        metrics_dict['drift_share'] = result['drift_share']
                        logger.info(f"   drift_share: {result['drift_share']}")
                    
                    # Drift by feature
                    if 'drift_by_columns' in result:
                        drift_by_columns = {}
                        for col_name, col_result in result['drift_by_columns'].items():
                            drift_by_columns[col_name] = {
                                'drift_score': col_result.get('drift_score', 0),
                                'drift_detected': col_result.get('drift_detected', False),
                                'statistical_test': col_result.get('stattest_name', col_result.get('stattest', 'unknown'))
                            }
                        metrics_dict['drift_by_feature'] = drift_by_columns
                        logger.info(f"   drift_by_columns: {len(drift_by_columns)} features")
                    else:
                        logger.warning("   'drift_by_columns' not found in result")
                    
                    # Number of drifted features
                    if 'number_of_drifted_columns' in result:
                        metrics_dict['number_of_drifted_features'] = result['number_of_drifted_columns']
                        logger.info(f"   number_of_drifted_columns: {result['number_of_drifted_columns']}")
                    else:
                        logger.warning("   'number_of_drifted_columns' not found")
                    
                    if 'number_of_columns' in result:
                        metrics_dict['total_features'] = result['number_of_columns']
                        logger.info(f"   number_of_columns: {result['number_of_columns']}")
                    else:
                        logger.warning("   'number_of_columns' not found")
                
                # ===== CLASSIFICATION METRICS =====
                elif 'Classification' in metric_type or 'classification' in metric_type.lower():
                    logger.info("  Processing Classification metric")
                    
                    # Target drift
                    if 'target_drift' in result:
                        metrics_dict['target_drift'] = result['target_drift']
                        logger.info(f"   target_drift: {result['target_drift']}")
                    
                    # Prediction drift
                    if 'prediction_drift' in result:
                        metrics_dict['prediction_drift'] = result['prediction_drift']
                        logger.info(f"  prediction_drift: {result['prediction_drift']}")
                    
                    # Performance metrics
                    if 'reference' in result and 'current' in result:
                        ref_metrics = result.get('reference', {})
                        curr_metrics = result.get('current', {})
                        
                        metrics_dict['performance'] = {
                            'reference': {
                                'accuracy': ref_metrics.get('accuracy', 0),
                                'precision': ref_metrics.get('precision', 0),
                                'recall': ref_metrics.get('recall', 0),
                                'f1': ref_metrics.get('f1', 0)
                            },
                            'current': {
                                'accuracy': curr_metrics.get('accuracy', 0),
                                'precision': curr_metrics.get('precision', 0),
                                'recall': curr_metrics.get('recall', 0),
                                'f1': curr_metrics.get('f1', 0)
                            }
                        }
                        logger.info(f"   performance metrics extracted")
        else:
            logger.warning("No 'metrics' key found in report_dict")
            
    except Exception as e:
        # If parsing fails, return basic metrics
        logger.error(f"Error parsing metrics: {e}", exc_info=True)
        metrics_dict['error'] = str(e)
    
    # Calculate overall drift score (0-1, higher = more drift)
    drift_score = 0.0
    if 'drift_by_feature' in metrics_dict and metrics_dict['drift_by_feature']:
        drifted_features = sum(
            1 for f in metrics_dict['drift_by_feature'].values() 
            if f.get('drift_detected', False)
        )
        total_features = len(metrics_dict['drift_by_feature'])
        if total_features > 0:
            drift_score = drifted_features / total_features
            logger.info(f"Calculated drift score: {drift_score} ({drifted_features}/{total_features})")
    else:
        logger.warning("No drift_by_feature data available to calculate drift score")
    
    metrics_dict['overall_drift_score'] = drift_score
    metrics_dict['drift_status'] = 'HIGH' if drift_score > 0.5 else 'MEDIUM' if drift_score > 0.2 else 'LOW'
    
    logger.info(f"Final drift status: {metrics_dict['drift_status']} (score: {drift_score})")
    
    return metrics_dict


def load_reference_data(file_path: str = "data_model/reference/reference_data.csv") -> pd.DataFrame:
    """
    Load reference/baseline data for drift monitoring.
    
    Args:
        file_path: Path to reference data CSV
        
    Returns:
        DataFrame with reference data
    """
    logger.info(f"Loading reference data from: {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"Reference data file not found: {file_path}")
        raise FileNotFoundError(f"Reference data not found: {file_path}")
    
    df = pd.read_csv(file_path)
    logger.info(f"Loaded reference data: {df.shape}")
    
    # Convert timestamp to datetime if exists
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        logger.info("Converted timestamp column to datetime")
    
    return df


def load_current_data(
    file_path: str = "data_model/production/production.csv",
    days: int = 30
) -> pd.DataFrame:
    """
    Load current production data for drift monitoring.
    
    Args:
        file_path: Path to production data CSV
        days: Number of recent days to include (default: 30)
        
    Returns:
        DataFrame with recent production data
    """
    logger.info(f"Loading current data from: {file_path} (last {days} days)")
    
    if not os.path.exists(file_path):
        logger.error(f"Production data file not found: {file_path}")
        raise FileNotFoundError(f"Production data not found: {file_path}")
    
    df = pd.read_csv(file_path)
    logger.info(f"Loaded production data: {df.shape}")
    
    # Convert timestamp to datetime and optionally filter by recent days
    if 'timestamp' in df.columns:
        # Convert, coerce errors to NaT for safety
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Only apply time filtering if we actually have valid timestamps
        if df['timestamp'].notna().any():
            from datetime import datetime, timedelta
            cutoff_date = datetime.now() - timedelta(days=days)
            before_shape = df.shape
            df = df[df['timestamp'] >= cutoff_date]
            logger.info(
                f"Filtered to data after {cutoff_date}: "
                f"{before_shape} -> {df.shape}"
            )
        else:
            logger.info(
                "Timestamp column exists but has no valid values; "
                "skipping time-based filtering and using all rows."
            )
    
    if len(df) == 0:
        logger.error(
            f"No production data available for drift analysis "
            f"(after optional time filtering, days={days})"
        )
        raise ValueError("No production data available for drift analysis")
    
    return df