"""
Docstring for model_pipeline.src.model.generic_trainer
"""
from pathlib import Path
import mlflow
from mlflow.models import infer_signature
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import (
    f1_score,
    confusion_matrix,
    ConfusionMatrixDisplay,
    roc_curve,
    auc,
)
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from catboost import CatBoostClassifier
from loguru import logger

from src.mlflow_utils.experiment_tracker import ExperimentTracker


class BinaryClassifierWrapper(mlflow.pyfunc.PythonModel):  #type:ignore
    """Generic wrapper for binary classification models"""
    
    def __init__(self, model, model_type, feature_names, label_encoder=None, feature_encoders=None):
        self.model = model
        self.model_type = model_type
        self.feature_names = feature_names
        self.label_encoder = label_encoder
        self.feature_encoders = feature_encoders or {}

    def predict(self, context, model_input, params=None):  #type:ignore
        df = model_input.copy()

        for col, encoder in self.feature_encoders.items():
            if col in df.columns:
                df[col] = encoder.transform(df[col].astype(str))
        
        X = df[self.feature_names]
        
        
        probs = self.model.predict_proba(X)[:, 1]
        
        
        if params is None:
            params = {}
        
        return_probs = params.get('return_probs', False)
        return_both = params.get('return_both', False)

        binary_preds = (probs >= 0.5).astype(int)
        if self.label_encoder is not None:
            final_preds = self.label_encoder.inverse_transform(binary_preds)
        else:
            final_preds = binary_preds

        max_probs = np.maximum(probs, 1 - probs)
        
        if return_both:
            return pd.DataFrame({
                'probability': max_probs,
                'prediction': final_preds
            })
        elif return_probs:
            return max_probs
        else:
            return final_preds


class GenericBinaryClassifierTrainer:
    """Generic trainer for multiple binary classification algorithms"""
    
    SUPPORTED_MODELS = {
        'random_forest': RandomForestClassifier,
        'decision_tree': DecisionTreeClassifier,
        'logistic_regression': LogisticRegression,
        'xgboost': XGBClassifier,
        'lightgbm': LGBMClassifier,
        'catboost': CatBoostClassifier,
    }
    
    def __init__(
        self,
        config: dict,
        experiment_tracker: ExperimentTracker,
        model_type: str 
    ):
        if model_type not in self.SUPPORTED_MODELS:
            raise ValueError(
                f"model_type must be one of {list(self.SUPPORTED_MODELS.keys())}, "
                f"got '{model_type}'"
            )
        
        self.config = config
        self.tracker = experiment_tracker
        self.model_type = model_type
        self.model = None
        self.feature_names = None
    
    def prepare_data(
        self,
        data: pd.DataFrame,
        target_col: str,
        feature_cols: list[str] | None = None,
        test_size: float = 0.2,
        random_state: int = 42
    ) -> tuple:
        logger.info(f"Preparing training data for {self.model_type}...")
        
        if feature_cols is None:
            feature_cols = [col for col in data.columns if col != target_col]
        
        self.feature_names = feature_cols
        X = data[feature_cols]
        y = data[target_col]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )

        logger.info(f"Training set: {X_train.shape}, Test set: {X_test.shape}")
        logger.info(f"Class distribution - Train: {y_train.value_counts().to_dict()}")
        logger.info(f"Class distribution - Test: {y_test.value_counts().to_dict()}")
        
        return X_train, X_test, y_train, y_test

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_test: pd.DataFrame,
        y_test: pd.Series,
        params: dict 
    ):
        """
        Train a binary classifier with mlflow tracking
        
        Args:
            X_train: Training features
            y_train: Training labels
            X_test: Test features
            y_test: Test labels
            params: Model-specific parameters (None = use config)
            
        Returns:
            Trained model
        """
        logger.info(f"Starting {self.model_type} model training...")
        
        if params is None:
            params = self.config.get(self.model_type, {})
        
        if self.feature_names is None:
            raise ValueError("Please prepare the data before training")

        # Disable post-training metrics and dataset logging to avoid
        # MLflow 3.x running expensive cross-validation internally
        mlflow.sklearn.autolog(  #type:ignore
            log_models=False,
            log_post_training_metrics=False,
            log_datasets=False,
        )

        model_class = self.SUPPORTED_MODELS[self.model_type]
        self.model = model_class(**params)

        if self.model_type == 'catboost':
            # Pass eval_set so od_type/od_wait/use_best_model work correctly
            self.model.fit(X_train, y_train, eval_set=(X_test, y_test))
        elif self.model_type == 'lightgbm':
            # Early stopping prevents overfitting; callbacks API avoids deprecation warning
            from lightgbm import early_stopping, log_evaluation
            self.model.fit(
                X_train, y_train,
                eval_set=[(X_test, y_test)],
                callbacks=[early_stopping(50, verbose=False), log_evaluation(period=-1)],
            )
        elif self.model_type == 'xgboost':
            self.model.fit(
                X_train, y_train,
                eval_set=[(X_test, y_test)],
                verbose=False,
            )
        else:
            self.model.fit(X_train, y_train)
        

        train_score = self.model.score(X_train, y_train)
        test_score = self.model.score(X_test, y_test)
        
        logger.info(f"Training complete.")
        logger.info(f"Train accuracy: {train_score:.4f}")
        logger.info(f"Test accuracy: {test_score:.4f}")
        
        self.tracker.log_metric("train_accuracy", train_score)
        self.tracker.log_metric("test_accuracy", test_score)

        training_f1 = f1_score(y_train, self.model.predict(X_train))
        self.tracker.log_metric("training_f1_score", training_f1)
        logger.info(f"Training F1 score: {training_f1:.4f}")
        

        self._log_feature_importance()
        self._log_plots(X_test, y_test)

        return self.model
    
    def _log_feature_importance(self):
        """Log feature importance metrics based on model type"""
        importance_dict = {}
        
        if self.model_type in ['random_forest', 'decision_tree', 'xgboost', 'lightgbm', 'catboost']:
            if hasattr(self.model, 'feature_importances_'):
                importance_dict = dict(zip(self.feature_names, self.model.feature_importances_))#type:ignore
        elif self.model_type == 'logistic_regression':
            if hasattr(self.model, 'coef_'):
                importance_dict = dict(zip(self.feature_names, np.abs(self.model.coef_[0])))#type:ignore
        
        if importance_dict:
            sorted_importance = sorted(
                importance_dict.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
            
            for feature, score in sorted_importance:
                self.tracker.log_metric(f"feature_importance/{feature}", score)  #type:ignore
            logger.info("Feature importance logged")
    
    def _log_plots(self, X_test: pd.DataFrame, y_test: pd.Series):
        """Generate and log confusion matrix + ROC curve to MLflow"""
        y_pred = self.model.predict(X_test)
        y_prob = self.model.predict_proba(X_test)[:, 1]

        # Confusion matrix
        cm_fig, ax = plt.subplots(figsize=(6, 5))
        ConfusionMatrixDisplay(confusion_matrix(y_test, y_pred)).plot(ax=ax, colorbar=False)
        ax.set_title(f"Confusion Matrix — {self.model_type}")
        mlflow.log_figure(cm_fig, "plots/confusion_matrix.png")
        plt.close(cm_fig)
        logger.info("Confusion matrix logged")

        # ROC curve
        fpr, tpr, _ = roc_curve(y_test, y_prob)
        roc_auc = auc(fpr, tpr)
        roc_fig, ax = plt.subplots(figsize=(6, 5))
        ax.plot(fpr, tpr, label=f"AUC = {roc_auc:.4f}")
        ax.plot([0, 1], [0, 1], "k--")
        ax.set_xlabel("False Positive Rate")
        ax.set_ylabel("True Positive Rate")
        ax.set_title(f"ROC Curve — {self.model_type}")
        ax.legend()
        mlflow.log_figure(roc_fig, "plots/roc_curve.png")
        plt.close(roc_fig)
        self.tracker.log_metric("roc_auc", roc_auc)
        logger.info(f"ROC curve logged (AUC={roc_auc:.4f})")

    def save_model(
        self,
        model_name: str,
        input_example: pd.DataFrame,
        label_encoder=None,
        feature_encoders=None
    ):
        """Save model with wrapper for consistent prediction interface"""
        if self.model is None:
            raise ValueError("Model not trained.")

        wrapper = BinaryClassifierWrapper(
            model=self.model,
            model_type=self.model_type,
            feature_names=self.feature_names,
            label_encoder=label_encoder,
            feature_encoders=feature_encoders
        )

        prediction = wrapper.predict(context=None, model_input=input_example)
        signature = infer_signature(input_example, prediction)
        
        SRC_PATH = Path(__file__).resolve().parents[2]
        logger.info(f"{SRC_PATH=}")
        
        logger.info(f"Saving {self.model_type} model as '{model_name}'...")
        mlflow.pyfunc.log_model(
            python_model=wrapper,
            artifact_path=model_name,
            signature=signature,
            input_example=input_example.iloc[:3],
            code_paths=[str(SRC_PATH / 'src')]
        )
    
    # def load_model(self, model_uri: str):
    #     """Load a trained model from MLflow"""
    #     logger.info(f"Loading model from {model_uri=}")
        
        
    #     self.model = mlflow.sklearn.load_model(model_uri) #type:ignore
        
    #     logger.info("Model loaded successfully")


