import os
import pickle
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
import numpy as np
import sys
from sklearn.model_selection import train_test_split
import pandas as pd
from pathlib import Path


def store(output_folder, X_train, X_test, y_train, y_test):
    os.makedirs(output_folder, exist_ok=True)
    with open(output_folder / 'X_train.pkl', 'wb') as f:
        pickle.dump(X_train, f)
    with open(output_folder / 'X_test.pkl', 'wb') as f:
        pickle.dump(X_test, f)
    with open(output_folder / 'y_train.pkl', 'wb') as f:
        pickle.dump(y_train, f)
    with open(output_folder / 'y_test.pkl', 'wb') as f:
        pickle.dump(y_test, f)
    logging.info("Data stored")


def read(artifacts_path):
    
    path = artifacts_path / 'data_sepal_processed.csv'
    df_sepal = pd.read_csv(path)
    path = artifacts_path / 'data_petal_processed.csv'
    df_petal = pd.read_csv(path)
    return df_sepal, df_petal


def main(artifacts_path):
    # Load the iris dataset
    artifacts_path = Path(artifacts_path)
    df_sepal, df_petal = read(artifacts_path)

    petal_features = ['petal length (cm)', 'petal width (cm)']
    sepal_features = ['sepal length (cm)', 'sepal width (cm)']
    all_features = petal_features + sepal_features
    for feature in petal_features:
        df_sepal[feature] = df_petal[feature]
    df = df_sepal.copy()
    X_train, X_test, y_train, y_test = train_test_split(df[all_features], df["target"])
    logging.info("Data red")
    # Merge sepal and petal features

    logging.info("Data Merged")
    store(artifacts_path, X_train, X_test, y_train, y_test)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processed_petal", default=Path("artifacts") / 'processed_petal'
    )
    parser.add_argument(
        "--processed_sepal", default=Path("artifacts") / 'processed_setal'
    )
    parser.add_argument(
        "--output_folder", default=Path("artifacts") / 'processed_data'
    )
    args = parser.parse_args()
    main(processed_petal=args.processed_petal,
         processed_sepal=args.processed_sepal,
         output_folder=args.output_folder)
