import sys
from pathlib import Path
import subprocess
import logging
import mlflow
from mlflow import MlflowClient
from model.train_pipeline import main as train_pipeline
client = MlflowClient()
mlflow.set_registry_uri("databricks-uc")


def install_requirements(requirements_path: str | None = None):
    """
    Install Python packages from a requirements.txt file.

    Args:
        requirements_path (str): Path to the requirements.txt file
    """
    if not requirements_path:
        requirements_path = "requirements.txt"
    requirements_path = Path(requirements_path)

    if not requirements_path.exists():
        logging.error(f"Requirements file not found: {requirements_path}")
        return

    logging.info(f"Installing packages from {requirements_path} ...")
    
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", str(requirements_path)])
        logging.info("All packages installed successfully!")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to install requirements: {e}")
        raise


def train(artifacts_path: str, experiment_name: str):
    install_requirements()
    mlflow.set_experiment(experiment_name) # TODO: if experiment exists then skip
    mlflow.autolog()
    with mlflow.start_run():
        train_pipeline(artifacts_path=artifacts_path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts_path", default="/Users/marioslioutas/brick-by-brick/artifacts")
    parser.add_argument("--experiment_name", default="ml_test")
    args = parser.parse_args()
    train(artifacts_path=args.artifacts_path, experiment_name=args.experiment_name)
