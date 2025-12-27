import glob
import os
import shutil
import sys
from pathlib import Path
import subprocess
import logging
import joblib
import mlflow
import pandas as pd
from mlflow.pyfunc import PythonModel
from mlflow.models.signature import ModelSignature
from mlflow import MlflowClient
from setuptools import find_packages, setup
from model.predict import signature, MyModel
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ServedModelInput,
    EndpointCoreConfigInput,
)
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from databricks.sdk.errors import NotFound
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


def build_wheel(
    wheel_name,
    version="0.1.0",
    package_dir=None,
    repo_root=".",
    artifacts_path=".",
):
    """Build a wheel and force its name regardless of folder/package structure."""
    original_argv = sys.argv
    original_cwd = os.getcwd()
    abs_repo_root = os.path.abspath(repo_root)
    target_dir = package_dir or "."
 
    print(f"🚀 Building wheel '{wheel_name}' from '{target_dir}'")
 
    try:
        os.chdir(abs_repo_root)
 
        # Clean old artifacts
        for p in ["dist", "build", "*.egg-info"]:
            for path in glob.glob(p):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                    print(f"   Removed: {path}")
                elif os.path.isfile(path):
                    os.remove(path)
                    print(f"   Removed: {path}")
 
        # Parse requirements
        req_file = Path("requirements.txt")
        requirements = []
        if req_file.exists():
            for line in req_file.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if wheel_name.replace("_", "-").lower() in line.replace("_", "-").lower():
                    print(f"   [FILTER] Excluding self-reference: {line}")
                    continue
                requirements.append(line)
 
        # Setup args
        sys.argv = ["setup.py", "bdist_wheel", "--dist-dir", str(artifacts_path)]

        setup(
            name=wheel_name,
            version=version,
            description="ERGO AT extraction pipelines",
            python_requires=">=3.10",
            packages=find_packages(where=target_dir),
            package_dir={"": target_dir},
            install_requires=requirements,
            include_package_data=False,
            zip_safe=False,
        )
 
    finally:
        os.chdir(original_cwd)
        sys.argv = original_argv
    # Verify output
    dist_path = os.path.abspath(artifacts_path)
    wheels = glob.glob(os.path.join(dist_path, "*.whl"))
    if not wheels:
        print("❌ No wheel created")
        return None

    new_wheel = wheels[0]
    print(f"📦 Created: {new_wheel}")
    return new_wheel


def register(artifacts_path: str, experiment_name: str, python_model: PythonModel, signature: ModelSignature, name: str, registered_model_name: str, code_paths: list[str] = [], pip_requirements: list[str] = [], deploy_endpoint: bool = True):
    artifacts_path = Path(artifacts_path)
    install_requirements()
    
    if (not code_paths) or (not pip_requirements):
        wheel_path = build_wheel(wheel_name=name)
        wheel_name = wheel_path.split("/")[-1]
        code_paths=[wheel_path]
        pip_requirements=[f"code/{wheel_name}"]

    mlflow.set_experiment(experiment_name)
    model_path = artifacts_path / "trained_model.pkl"
    result = mlflow.pyfunc.log_model(
        name=name,
        registered_model_name=registered_model_name,
        signature=signature,
        python_model=python_model(),
        artifacts={"model": str(model_path)},
        code_paths=code_paths,
        pip_requirements=pip_requirements,
    )
    model_uri = f"models:/{registered_model_name}/{result.registered_model_version}"
    print(f"✅ Model registered: {model_uri}")
    if deploy_endpoint:
        create_or_update_endpoint(
            endpoint_name=name,
            model_name=registered_model_name,
            model_version=result.registered_model_version,
        )


def create_or_update_endpoint(
    endpoint_name: str,
    model_name: str,  # This should be the registered model name in MLflow Model Registry
    model_version: int,
):
    w = WorkspaceClient()

    # Create the served entity
    served_entity = ServedEntityInput(
        entity_name=model_name,  # This must be an existing registered model name
        entity_version=model_version,
        name=endpoint_name,
        workload_size="Small",
        scale_to_zero_enabled=True,
    )

    # Create or update the endpoint
    try:
        endpoint = w.serving_endpoints.get(endpoint_name)
        print(f"🔄 Endpoint '{endpoint_name}' exists → updating")
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_entities=[served_entity],
        )
    except NotFound:
        print(f"🚀 Creating endpoint '{endpoint_name}'")
        w.serving_endpoints.create(
            name=endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=[served_entity]
            ),
        )

    print(
        f"✅ Endpoint '{endpoint_name}' now serves "
        f"{model_name} v{model_version} "
    )


    # served_model = ServedModelInput(
    #     name=served_model_name,
    #     model_name=model_name,
    #     model_version=model_version,
    #     workload_size="Small",
    #     scale_to_zero_enabled=True
    # )

    # try:
    #     # 1️⃣ Check if endpoint exists
    #     endpoint = w.serving_endpoints.get(endpoint_name)

    #     print(f"🔄 Endpoint '{endpoint_name}' exists → updating")

    #     w.serving_endpoints.update_config(
    #         name=endpoint_name,
    #         served_models=[served_model],
    #     )

    # except NotFound:
    #     # 2️⃣ Create endpoint if it does not exist
    #     print(f"🚀 Creating endpoint '{endpoint_name}'")

    #     w.serving_endpoints.create(
    #         name=endpoint_name,
    #         config=EndpointCoreConfigInput(
    #             served_models=[served_model]
    #         ),
    #     )

    # print(
    #     f"✅ Endpoint '{endpoint_name}' now serves "
    #     f"{model_name} v{model_version} as '{served_model_name}'"
    # )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="ml_test_model")
    parser.add_argument("--artifacts_path", default="/Users/marioslioutas/brick-by-brick/artifacts")
    parser.add_argument("--experiment_name", default="ml_test")

    parser.add_argument("--registered_model_name", default="registered_model_name")
    args = parser.parse_args()
    register(
        artifacts_path=args.artifacts_path,
        python_model=MyModel, 
        signature=signature, 
        name=args.name, 
        registered_model_name=args.registered_model_name,
        experiment_name=args.experiment_name
    )