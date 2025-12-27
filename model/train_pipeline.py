import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from model.get_data import main as get_data
from model.process_petal import main as process_petal
from model.process_sepal import main as process_sepal
from model.merge_processed_data import main as merge_processed_data
from model.train_model import main as train_model
from model.predict import main as predict


def main(artifacts_path: str):
    get_data(artifacts_path)
    process_petal(artifacts_path)
    process_sepal(artifacts_path)
    merge_processed_data(artifacts_path)
    train_model(artifacts_path)


if __name__ == "__main__":
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts_path", default="/Users/marioslioutas/brick-by-brick/artifacts")
    args = parser.parse_args()
    main(artifacts_path=args.artifacts_path)
