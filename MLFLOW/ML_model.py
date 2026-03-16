from pyspark.sql import functions as F
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from xgboost import XGBClassifier
from imblearn.over_sampling import SMOTE
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
    classification_report,
    average_precision_score
)


df = spark.table("content_job.gold.account_master_view")

columns_to_drop = [
    "account_id",
    "account_name",
    "accountAgeCategory",
    "account_creation_year_month",
    "verificationConfidence"
]

df = df.select([c for c in df.columns if c not in columns_to_drop])
df = df.withColumn(
    "is_group",
    F.when(F.col("is_group") == True, 1).otherwise(0)
)

pdf = df.toPandas()

X = pdf.drop(columns=["potentialBot"])
y = pdf["potentialBot"]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    stratify=y,
    random_state=42
)

imputer = SimpleImputer(strategy="median")

X_train = pd.DataFrame(
    imputer.fit_transform(X_train),
    columns=X_train.columns
)

X_test = pd.DataFrame(
    imputer.transform(X_test),
    columns=X_test.columns
)

scaler = StandardScaler()

X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

smote = SMOTE(random_state=42)

X_train_bal, y_train_bal = smote.fit_resample(X_train_scaled, y_train)

models = {

    "XGBoost": (
        XGBClassifier(
            random_state=42,
            eval_metric="logloss",
            tree_method="hist"
        ),
        {
            "n_estimators": [200, 400],
            "max_depth": [3, 6],
            "learning_rate": [0.01, 0.05]
        }
    ),

    "RandomForest": (
        RandomForestClassifier(random_state=42),
        {
            "n_estimators": [200, 400],
            "max_depth": [5, 10]
        }
    ),

    "LogisticRegression": (
        LogisticRegression(max_iter=2000),
        {
            "C": [0.01, 0.1, 1, 10]
        }
    ),

    "SVM": (
        SVC(probability=True),
        {
            "C": [0.1, 1, 10],
            "kernel": ["rbf", "linear"]
        }
    )
}

best_model = None
best_auc = 0

for model_name, (model, params) in models.items():
    print(f"Training model: {model_name}")
    grid_search = GridSearchCV(
        model,
        params,
        scoring="roc_auc",
        cv=3,
        n_jobs=-1,
        verbose=1
    )

    with mlflow.start_run(run_name=f"BotDetection_{model_name}"):
        grid_search.fit(X_train_bal, y_train_bal)
        best_estimator = grid_search.best_estimator_
        probabilities = best_estimator.predict_proba(X_test_scaled)[:, 1]
        predictions = (probabilities > 0.5).astype(int)
        accuracy = accuracy_score(y_test, predictions)
        precision = precision_score(y_test, predictions)
        recall = recall_score(y_test, predictions)
        f1 = f1_score(y_test, predictions)
        roc_auc = roc_auc_score(y_test, probabilities)
        pr_auc = average_precision_score(y_test, probabilities)

        print(f"Accuracy  : {accuracy:.4f}")
        print(f"Precision : {precision:.4f}")
        print(f"Recall    : {recall:.4f}")
        print(f"F1 Score  : {f1:.4f}")
        print(f"ROC AUC   : {roc_auc:.4f}")
        print(f"PR AUC    : {pr_auc:.4f}")
        
        cm = confusion_matrix(y_test, predictions)
        print("\nConfusion Matrix")
        print(cm)

        print("\nClassification Report")
        print(classification_report(y_test, predictions))

        mlflow.log_param("model", model_name)
        mlflow.log_params(grid_search.best_params_)

        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("roc_auc", roc_auc)
        mlflow.log_metric("pr_auc", pr_auc)

        mlflow.sklearn.log_model(
            best_estimator,
            name=f"{model_name}_model",
            input_example=X_train[:5]
        )

        if roc_auc > best_auc:
            best_auc = roc_auc
            best_model = best_estimator



