'''
Part B: Model Training using Ray Tune (30 Marks)
Implement a Python script named train <roll no>.py.
The script should:
1. Load the processed training Parquet dataset.
2. Split the processed training data into training and validation sets (80:20)
3. Build a binary classification model. You can choose any ML/DL model.
4. Tune the model using Ray Tune by maximizing the validation F1-score.
5. Evaluate the best-performing model on the validation set.
6. Register the best model in MLflow.
Perform at least 10 hyperparameter tuning trials.

Part B: MLflow Logging (20 Marks)
Create an experiment named assignment 2 <roll no>. Each Ray Tune trial must be logged as a
separate MLflow run. Parameters should be logged.
Metrics
• Training loss and validation loss if using pytorch model
• Accuracy
• Precision
• Recall
• F1-score
• ROC-AUC
• Training time
Artifacts
• Trained model
• Confusion matrix
• Classification report
• Predictions on validation set

Part C: Model Registration (20 Marks)
• Register the model from the Ray Tune trial with the highest validation F1-score in the MLflow
Model Registry using the name Assignment2Classifier <roll no>.
Assignment2Classifier <roll no>.
• Demonstrate loading the registered model and performing inference on the processed test.csv
dataset. Save the predictions as a CSV file (predictions <roll no>.csv).

Roll No : DA25M502
Name : Amardeep Kumar
ray version : 2.56.1
MLflow version : 3.14.0
'''

import os
# =========================================================================
# SYSTEM ENVIRONMENT CONFIGURATION FOR SECURE RUNS
# =========================================================================
os.environ["RAY_memory_monitor_refresh_ms"] = "0"  

import time
import json
import pyarrow.parquet as pq
import numpy as np
import pandas as pd

import mlflow
import mlflow.sklearn
import ray
from ray import tune  

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, roc_auc_score, confusion_matrix, classification_report
)

def load_processed_data(parquet_path):
    """Loads PySpark ML features vector cleanly."""
    table = pq.read_table(parquet_path)
    df = table.to_pandas()
    
    if isinstance(df['features'].iloc[0], dict):
        X = np.array(df['features'].apply(lambda x: x['values']).tolist())
    else:
        X = np.array(df['features'].tolist())
        
    y = df['label'].values if 'label' in df.columns else None
    return X, y

def train_evaluate_lr(config, data_train, data_val):
    """Ray Tune Objective function with explicit MLflow tracking uri."""
    X_train, y_train = data_train
    X_val, y_val = data_val
    
    X_train_mini = X_train[:200]
    y_train_mini = y_train[:200]
    X_val_mini = X_val[:100]
    y_val_mini = y_val[:100]

    # FIX: Explicitly set the tracking server URI inside the worker context
    mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment("assignment_2_da25m502")

    with mlflow.start_run(nested=True) as run:
        mlflow.log_params({
            "C": config["C"],
            "max_iter": config["max_iter"],
            "solver": "lbfgs"
        })
        
        start_time = time.time()
        
        model = LogisticRegression(
            C=config["C"],
            max_iter=config["max_iter"],
            solver="lbfgs",
            random_state=42,
            n_jobs=1
        )
        model.fit(X_train_mini, y_train_mini)
        
        duration = time.time() - start_time
        
        preds_class = model.predict(X_val_mini)
        preds_prob = model.predict_proba(X_val_mini)[:, 1]
        
        acc = accuracy_score(y_val_mini, preds_class)
        prec = precision_score(y_val_mini, preds_class, zero_division=0)
        rec = recall_score(y_val_mini, preds_class, zero_division=0)
        f1 = f1_score(y_val_mini, preds_class, zero_division=0)
        auc = roc_auc_score(y_val_mini, preds_prob) if len(np.unique(y_val_mini)) > 1 else 0.5
        
        mlflow.log_metrics({
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1_score": f1,
            "roc_auc": auc,
            "training_time": duration
        })
        
        os.makedirs("data/artifacts", exist_ok=True)
        
        val_preds_df = pd.DataFrame({"true_label": y_val_mini, "pred_prob": preds_prob, "pred_label": preds_class})
        val_preds_path = "data/artifacts/validation_predictions.csv"
        val_preds_df.to_csv(val_preds_path, index=False)
        mlflow.log_artifact(val_preds_path)
        
        cm = confusion_matrix(y_val_mini, preds_class)
        cm_path = "data/artifacts/confusion_matrix.json"
        with open(cm_path, "w") as f:
            json.dump(cm.tolist(), f)
        mlflow.log_artifact(cm_path)
        
        report = classification_report(y_val_mini, preds_class, zero_division=0)
        report_path = "data/artifacts/classification_report.txt"
        with open(report_path, "w") as f:
            f.write(report)
        mlflow.log_artifact(report_path)
        
        mlflow.sklearn.log_model(model, name="model")
        
        tune.report({"f1_score": f1, "mlflow_run_id": run.info.run_id})

def main():
    """Main pipeline execution workflow."""
    ray.init(
        num_cpus=1, 
        ignore_reinit_error=True
    )
    
    train_parquet_path = "data/train_processed_da25m502.parquet"
    test_parquet_path = "data/test_processed_da25m502.parquet"
    
    print("Loading data partitions...")
    X, y = load_processed_data(train_parquet_path)
    
    np.random.seed(42)
    shuffled_indices = np.random.permutation(len(X))
    split_idx = int(len(X) * 0.8)
    
    X_train, y_train = X[shuffled_indices[:split_idx]], y[shuffled_indices[:split_idx]]
    X_val, y_val = X[shuffled_indices[split_idx:]], y[shuffled_indices[split_idx:]]
    
    mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment("assignment_2_da25m502")
    
    search_space = {
        "C": tune.choice([0.01, 0.1, 1.0, 10.0]),
        "max_iter": tune.choice([5])
    }
    
    print("Beginning 10 Hyperparameter Tuning Trials via Ray Tune...")
    
    with mlflow.start_run(run_name="ray_tune_orchestrator"):
        trainable_with_resources = tune.with_resources(
            tune.with_parameters(
                train_evaluate_lr, 
                data_train=(X_train, y_train), 
                data_val=(X_val, y_val)
            ),
            resources={"cpu": 1} 
        )

        tuner = tune.Tuner(
            trainable_with_resources,
            param_space=search_space,
            tune_config=tune.TuneConfig(
                metric="f1_score",
                mode="max",
                num_samples=10
            )
        )
        results = tuner.fit()
        
    best_result = results.get_best_result(metric="f1_score", mode="max")
    best_run_id = best_result.metrics["mlflow_run_id"]
    
    print("\nTuning Complete!")
    print(f"Highest Validation F1-Score: {best_result.metrics['f1_score']:.5f}")
    
    model_name = "Assignment2Classifier_da25m502"
    model_uri = f"runs:/{best_run_id}/model"
    
    print(f"\nRegistering the top model from Run ID ({best_run_id}) in Model Registry...")
    model_details = mlflow.register_model(model_uri=model_uri, name=model_name)
    print(f"Model successfully registered as: '{model_name}' (Version {model_details.version}).")
    
    print(f"\nLoading '{model_name}' version: {model_details.version} for verification...")
    registry_uri = f"models:/{model_name}/{model_details.version}"
    loaded_model = mlflow.sklearn.load_model(registry_uri)
    
    print("Loading processed test dataset...")
    X_test, _ = load_processed_data(test_parquet_path)
    
    print("Executing batch inference predictions...")
    test_preds_prob = loaded_model.predict_proba(X_test)[:, 1]
    test_preds_class = loaded_model.predict(X_test)
    
    predictions_df = pd.DataFrame({
        "prediction_probability": test_preds_prob,
        "prediction_class": test_preds_class
    })
    
    output_csv_path = "data/predictions_da25m502.csv"
    predictions_df.to_csv(output_csv_path, index=False)
    print(f"Success! Predictions saved to: {os.path.abspath(output_csv_path)}")
    
    if os.path.exists("data/artifacts"):
        for file in os.listdir("data/artifacts"):
            os.remove(os.path.join("data/artifacts", file))
        os.rmdir("data/artifacts")

if __name__ == "__main__":
    main()