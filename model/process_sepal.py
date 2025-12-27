import os
import pickle
import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
import sys
import pandas as pd
from pathlib import Path


def store(output_folder, df):
    os.makedirs(output_folder, exist_ok=True)
    path = output_folder / 'data_sepal_processed.csv'
    df.to_csv(path, index=False)
    logging.info("Data stored")


def read(input_folder):
    # Load the train and test data
    path = input_folder / 'data_raw.csv'
    df = pd.read_csv(path)
    return df

def main(artifacts_path):
    artifacts_path =  Path(artifacts_path)
    # Load the iris dataset
    df = read(artifacts_path)
    logging.info("Data red")

    sepal_features = ['sepal length (cm)', 'sepal width (cm)']

    # Process only the sepal features
    for feature in sepal_features:
        df[feature] = (df[feature] - df[feature].mean(axis=0)) / df[feature].std(axis=0)

    logging.info("Sepal Data processed")

    store(artifacts_path, df)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_folder", default=Path("artifacts") / 'raw_data'
    )
    parser.add_argument(
        "--output_folder", default=Path("artifacts") /  'processed_sepal'
    )
    args = parser.parse_args()
    main(input_folder=args.input_folder, output_folder=args.output_folder)

