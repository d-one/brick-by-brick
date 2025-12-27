import numpy as np
from sklearn.ensemble import RandomForestClassifier
import pickle
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
import os
import pickle
import os, sys
from pathlib import Path


def store(output_folder, clf):
    os.makedirs(output_folder, exist_ok=True)
    path = output_folder / 'trained_model.pkl'
    with open(path, 'wb') as f:
        pickle.dump(clf, f)
    logging.info("Model stored")


def read(processed_data):
    # Load the merged train and test data
    with open(processed_data / 'X_train.pkl', 'rb') as f:
        X_train = pickle.load(f)
    with open(processed_data / 'y_train.pkl', 'rb') as f:
        y_train = pickle.load(f)
    return X_train, y_train


def main(artifacts_path):
    # Load the iris dataset
    artifacts_path = Path(artifacts_path)
    X_train, y_train = read(artifacts_path)
    logging.info("Data red")
    # Train a Random Forest model
    clf = RandomForestClassifier(random_state=42)
    clf.fit(X_train, y_train)
    logging.info("Model Trained")
    store(artifacts_path, clf)
