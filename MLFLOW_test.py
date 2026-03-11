from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
    classification_report
)

# Load data
df = spark.table("content.target.gold_account_master_view")
columns_to_drop = ["account_name", "verificationConfidence", "isVerified"]
df = df.select([c for c in df.columns if c not in columns_to_drop])

df_clean = df.dropna()
indexer = StringIndexer(
    inputCol="accountAgeCategory",
    outputCol="accountAgeCategory_index"
)

df_indexed = indexer.fit(df_clean).transform(df_clean)
df_indexed = df_indexed.withColumn(
    "potentialInfluencer",
    F.when(F.col("potentialInfluencer") == True, 1).otherwise(0)
)

df_indexed = df_indexed.drop("accountAgeCategory")
pdf = df_indexed.toPandas()

X = pdf.drop(columns=["potentialInfluencer"])
y = pdf["potentialInfluencer"]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    stratify=y,
    random_state=42
)

with mlflow.start_run(run_name="Bot_Detection_v1"):
    model = XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate = 0.05,
        scale_pos_weight=2.05
    )

    model.fit(X_train, y_train)

predictions = model.predict(X_test)
probabilities = model.predict_proba(X_test)[:, 1]

accuracy = accuracy_score(y_test, predictions)
precision = precision_score(y_test, predictions)
recall = recall_score(y_test, predictions)
f1 = f1_score(y_test, predictions)
roc_auc = roc_auc_score(y_test, probabilities)

print("Metrics")
print(f"Accuracy: {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
print(f"F1 Score: {f1:.4f}")
print(f"ROC AUC: {roc_auc:.4f}")

cm = confusion_matrix(y_test, predictions)
print("\nConfusion Matrix")
print(cm)

print("\nClassification Report")
print(classification_report(y_test, predictions))

mlflow.log_metric("accuracy", accuracy)
mlflow.log_metric("precision", precision)
mlflow.log_metric("recall", recall)
mlflow.log_metric("f1_score", f1)
mlflow.log_metric("roc_auc", roc_auc)
mlflow.sklearn.log_model(model, "model", input_example=X_train.head(5))







