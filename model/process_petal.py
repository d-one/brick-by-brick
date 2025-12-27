import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
import os
import pickle
import sys
import pandas as pd
from pathlib import Path


def store(output_folder, df):
    os.makedirs(output_folder, exist_ok=True)
    path = output_folder / 'data_petal_processed.csv'
    df.to_csv(path, index=False)
    logging.info("Data stored")


def read(input_folder):
    path = input_folder / 'data_raw.csv'
    df = pd.read_csv(path)
    return df

def main(artifacts_path):
    artifacts_path = Path(artifacts_path)
    # Load the iris dataset
    df = read(artifacts_path)
    logging.info("Data red")
    # Process only the petal features (last two columns)
    petal_features = ['petal length (cm)', 'petal width (cm)']
    # Process only the sepal features
    for feature in petal_features:
        df[feature] = df[feature] ** 2
    logging.info("Petal Data processed")
    store(artifacts_path, df)



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_folder", default=Path("artifacts") / 'raw_data'
    )
    parser.add_argument(
        "--output_folder", default=Path("artifacts") /  'processed_petal'
    )
    args = parser.parse_args()
    main(input_folder=args.input_folder, output_folder=args.output_folder)
