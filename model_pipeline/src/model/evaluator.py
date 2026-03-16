"""
Docstring for model_pipelinene.src.mlflow_utils.evaluator
"""
import mlflow
import mlflow.models
import numpy as np
import pandas as pd
from loguru import logger
from mlflow.models import MetricThreshold

from src.mlflow_utils.experiment_tracker import ExperimentTracker


class ModelEvaluator:
    def __init__(self, config: dict, experiment_tracker: ExperimentTracker):
        self.config = config
        self.tracker = experiment_tracker
        self.evaluation_results = None
    
    def evaluate_model(
        self,
        model_uri: str,
        eval_data: pd.DataFrame,
        target_col: str,
        model_type: str = "classifier",
    ) -> dict:
        """
        Evaluate model using MLflow's evaluate API
        
        Args:
            model_uri: MLflow model URI
            eval_data: Evaluation dataset with features and target
            target_col: Target column name
            model_type: "classifier" or "regressor"
            
        Returns:
            Dictionary of evaluation metrics
        """
        logger.info(f"Evaluating model: {model_uri}")
        logger.info(f"Evaluation data shape: {eval_data.shape}")

        evaluator_config = {}
        if self.config.get('shap', {}).get('enable', False):
            evaluator_config["log_explainer"] = True
            evaluator_config["explainer_type"] = self.config["shap"].get(
                "explainer_type", "exact"
            )
            evaluator_config["max_error_examples"] = self.config["shap"].get(
                "max_samples", 100
            )
        
        
        self.evaluation_results = mlflow.models.evaluate(
            model=model_uri,
            data=eval_data,
            targets=target_col,
            model_type=model_type,
            evaluator_config=evaluator_config,
        )
        metrics = self.evaluation_results.metrics
        logger.info("Evaluation complete")
        logger.info(f"Metrics: {metrics}")
        
        return metrics

    def validate_against_threshold(
        self,
        metrics: dict[str, float] | None = None
    ):
        """
        Validate metrics against configured thresholds
        """
        if metrics is None:
            if self.evaluation_results is None:
                raise ValueError("No evaluation results available")
            metrics = self.evaluation_results.metrics
        
        thresholds_config = self.config.get("thresholds", {})
        if not thresholds_config:
            logger.info("No thresholds configured, skipping validation")
            return True
        
        logger.info("Validating metrics against thresholds...")

        thresholds = {}
        for metric_name, threshold_value in thresholds_config.items():
            if metric_name in metrics:
                thresholds[metric_name] = MetricThreshold(
                    threshold=threshold_value,
                    greater_is_better=True
                )
        try:
            mlflow.validate_evaluation_results(
                candidate_result=self.evaluation_results,#type:ignore
                validation_thresholds=thresholds,
            )
            logger.info("All thresholds met!")
            return True
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False
    

    def compare_models(
        self,
        baseline_model_uri: str,
        candidate_model_uri: str,
        eval_data: pd.DataFrame,
        target_col: str,
    ) -> dict:
        logger.info("Comparing models...")
        logger.info("Evaluating baseline model...")
        
        baseline_results = mlflow.models.evaluate(
            model=baseline_model_uri,
            data=eval_data,
            targets=target_col,
            model_type="classifier",
        )
        logger.info("Evaluating candidate model...")
        candidate_results = mlflow.models.evaluate(
            model=candidate_model_uri,
            data=eval_data,
            targets=target_col,
            model_type="classifier",
        )
        

        comparison = {}
        comparison_flat = {}
        for metric in baseline_results.metrics.keys():
            baseline_value = baseline_results.metrics.get(metric, 0)
            candidate_value = candidate_results.metrics.get(metric, 0)
            improvement = candidate_value - baseline_value

            comparison[metric] = {
                "baseline": baseline_value,
                "candidate": candidate_value,
                "improvement": improvement,
                "improvement_pct": (improvement / baseline_value * 100) if baseline_value != 0 else 0,
            }
        
        

        logger.info("Model comparison complete")
        for metric, values in comparison.items():
            logger.info(
                f"{metric}: baseline={values['baseline']:.4f}, "
                f"candidate={values['candidate']:.4f}, "
                f"improvement={values['improvement']:+.4f} ({values['improvement_pct']:+.2f}%)"
            )
        
        self.tracker.log_dict(comparison, f"model_comparision_baseline_{baseline_model_uri}_candicate_{candidate_model_uri}.json")
        
        comparison_flat = {}
        for metric, values in comparison.items():
            comparison_flat[f"delta_{metric}"] = values["improvement"]
            
        self.tracker.log_metrics(comparison_flat) 
        
        return comparison
    
    def get_metrics_summary(self) -> pd.DataFrame:
        """
        Get summary of evaluation metrics
        
        Returns:
            DataFrame with metrics summary
        """
        if self.evaluation_results is None:
            raise ValueError("No evaluation results available")
        
        metrics_df = pd.DataFrame([self.evaluation_results.metrics])
        return metrics_df.T.rename(columns={0: "value"})
        