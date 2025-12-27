import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
import os
from sklearn.datasets import load_iris
import pandas as pd
from pathlib import Path


def store(artifacts_path, df):
    os.makedirs(artifacts_path, exist_ok=True)
    path = artifacts_path / 'data_raw.csv'
    df.to_csv(path, index=False)
    logging.info("Data stored")


def read():
    # Load the train and test data
    data = load_iris()
    return data

def main(artifacts_path):
    artifacts_path = Path(artifacts_path)
    # Load the iris dataset
    data = read()
    logging.info("Data red")
    df = pd.DataFrame(data.data, columns=data.feature_names)
    df['target'] = data.target
    logging.info("Data processed")
    store(artifacts_path, df)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_folder", default=Path("artifacts") / 'raw_data'
    )
    args = parser.parse_args()
    main(output_folder=args.output_folder)
