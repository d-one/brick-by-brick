import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from sklearn.datasets import load_iris
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
import os
import pickle
from sklearn.metrics import accuracy_score, classification_report
import mlflow
import os, sys
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
import mlflow.pyfunc
from mlflow.models.signature import ModelSignature
from mlflow.types import Schema, ColSpec
from mlflow.pyfunc import PythonModel


# Define signature
input_schema = Schema([
    ColSpec("double", "sepal length (cm)"),
    ColSpec("double", "sepal width (cm)"),
    ColSpec("double", "petal length (cm)"),
    ColSpec("double", "petal width (cm)"),
])

output_schema = Schema([
    ColSpec("string")  # Returning dicts as JSON-like objects
])

signature = ModelSignature(inputs=input_schema, outputs=output_schema)

class MyModel(PythonModel):
    def load_context(self, context):
        # Load model from artifacts
        with open(context.artifacts["model"], "rb") as f:
            self.model = pickle.load(f)
        
        self.feature_names = [
            "sepal length (cm)",
            "sepal width (cm)", 
            "petal length (cm)",
            "petal width (cm)",
        ]
        self.class_names = ["setosa", "versicolor", "virginica"]
    
    def predict(self, context, model_input: pd.DataFrame) -> pd.Series:
        # Ensure correct column order
        X = model_input[self.feature_names].to_numpy()
        
        # Get probabilities
        probs = self.model.predict_proba(X)
        
        # Convert each row to a dict with probabilities + predicted class + confidence
        results = [
            {
                **{cls: float(prob) for cls, prob in zip(self.class_names, row)},
                "predicted_class": self.class_names[np.argmax(row)],
                "confidence": float(np.max(row))
            }
            for row in probs
        ]
        
        return pd.Series(results)





def store(output_folder, metrics):
    os.makedirs(output_folder, exist_ok=True)
    output_folder = Path(output_folder)
    with open(output_folder / 'metrics.pkl', 'wb') as f:
        pickle.dump(metrics, f)
    logging.info("Metrics stored")


def read(artifacts_path):
    # Load the merged test data
    with open(artifacts_path / 'X_test.pkl', 'rb') as f:
        X_test = pickle.load(f)

    with open(artifacts_path / 'y_test.pkl', 'rb') as f:
        y_test = pickle.load(f)

    # Load the trained model
    with open(artifacts_path / 'trained_model.pkl', 'rb') as f:
        clf = pickle.load(f)
    return X_test, y_test, clf


def main(artifacts_path):
    artifacts_path = Path(artifacts_path)
    # Load the iris dataset
    X_test, y_test, clf = read(artifacts_path)
    logging.info("Data red")
    # Make predictions
    y_pred = clf.predict(X_test)

    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    # Save the metrics as pickle
    metrics = {
        'accuracy': accuracy,
        'classification_report': report
    }
    logging.info(f'accuracy: {accuracy}')
    logging.info("Test metrics calculated")

    visualize_folder = artifacts_path / "visualizations"
    os.makedirs(visualize_folder, exist_ok=True)
    plot_path = os.path.join(visualize_folder, 'iris_data_visualization.png')
    visualize(plot_path, X_test, y_test)

    mlflow.start_run()

    mlflow.log_metric("Accuracy", accuracy)
    import datetime
    mlflow.log_metric("minute", datetime.datetime.now().minute)
    mlflow.log_artifact(plot_path)

    mlflow.end_run()
    store(artifacts_path, metrics)


def visualize(plot_path, X_test, y_test):
    iris = load_iris()
    class_names = iris.target_names
    feature_names = iris.feature_names
    # Create a DataFrame for visualization
    df = pd.DataFrame(X_test, columns=feature_names)
    df['species'] = [class_names[i] for i in y_test]
    # Plot with Seaborn for better visuals
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=df[feature_names[0]], y=df[feature_names[2]], hue=df['species'], palette="deep", s=100)
    # Set plot labels and title
    plt.title('Iris Dataset Visualization: Sepal Length vs Petal Length')
    plt.xlabel(feature_names[0])
    plt.ylabel(feature_names[2])
    plt.legend(title='Species')
    plt.grid(True)
    # Save the plot locally
    plt.savefig(plot_path)
    plt.close()


def inference_example(artifacts_path):
    artifacts_path = Path(artifacts_path)
    from mlflow.pyfunc import PythonModelContext

    context = PythonModelContext(
        artifacts={
            "model": artifacts_path / "trained_model.pkl"   # path to your pickled sklearn model
        },
        model_config={}
    )
    model = MyModel()          # __init__()
    model.load_context(context)  # MANUAL call (only in this case)
    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "sepal length (cm)": 5.1,
                "sepal width (cm)": 3.5,
                "petal length (cm)": 1.4,
                "petal width (cm)": 0.2,
            }
        ],
        columns=[
                "sepal length (cm)",
                "sepal width (cm)", 
                "petal length (cm)",
                "petal width (cm)"
            ]
        )

    preds = model.predict(None, df)
    print(preds)




if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts_path", default="/Users/marioslioutas/brick-by-brick/artifacts")
    args = parser.parse_args()
    # main(
    #     artifacts_path=args.artifacts_path
    # )
    inference_example(artifacts_path=args.artifacts_path)



