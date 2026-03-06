"""
DAG: daily_batch_feature_pipeline
==================================
Runs ONCE A DAY (at midnight UTC by default).

What it does:
  - Spins up the exact same Spark container that the `batch_feature_pipeline`
    compose service uses.
  - Runs the batch pipeline end-to-end (feature engineering + writes to the
    offline store + calls materialization into Redis at the very end).
  - No separate materialization step is needed here because the pipeline
    script already handles it internally.

Schedule:
  Adjust `schedule_interval` below to match your preferred daily window,
  e.g. "0 2 * * *" for 02:00 UTC.
"""

import os
import pendulum
from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ---------------------------------------------------------------------------
# Configuration — keep in sync with your compose.yaml
# HOST_PROJECT_DIR: absolute path on the HOST machine (not in the container).
# ---------------------------------------------------------------------------
HOST_PROJECT_DIR = Path(os.environ["HOST_PROJECT_DIR"])
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "batch_feature_pipeline:latest")
DOCKER_NETWORK = "anomaly-detection-network"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=10),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_batch_feature_pipeline",
    description="Runs the Spark batch feature pipeline once a day.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",   # or e.g. "0 2 * * *" for 02:00 UTC
    catchup=False,                # Don't back-fill missed runs
    max_active_runs=1,            # Never run two instances in parallel
    default_args=default_args,
    tags=["feature-pipeline", "batch", "daily"],
) as dag:

    # ------------------------------------------------------------------
    # Task — Run the Spark batch pipeline
    #
    # Mirrors the `batch_feature_pipeline` compose service exactly:
    #   - same image
    #   - same command
    #   - same environment variables
    #   - same volume mounts
    #   - same shared memory (2 GB for Spark)
    # ------------------------------------------------------------------
    run_batch_pipeline = DockerOperator(
        task_id="run_batch_feature_pipeline",
        image=SPARK_IMAGE,
        # Entrypoint is `python`, command becomes the module to run
        command=["-m", "batch_pipeline_service.src.batch_pipeline"],
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        environment={
            "APP_HOME": "/app",
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": "/app",
            "SPARK_DRIVER_MEMORY": "2g",
            "CONFIG_PATH": "/app/batch_pipeline_service/src/config.yaml",
        },
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_DIR}/data",
                target="/app/data",
                type="bind",
            ),
            Mount(
                source=f"{HOST_PROJECT_DIR}/services/batch_pipeline_service",
                target="/app/batch_pipeline_service",
                type="bind",
            ),
            Mount(
                source=f"{HOST_PROJECT_DIR}/services/data_engineering_service",
                target="/app/data_engineering_service",
                type="bind",
            ),
            Mount(
                source=f"{HOST_PROJECT_DIR}/services/feature_store_service/src",
                target="/app/feature_store_service",
                type="bind",
                read_only=True,
            ),
        ],
        shm_size="2g",           # Spark needs extra shared memory
        auto_remove="success",
        mount_tmp_dir=False,
        # Give Spark enough time to complete — adjust if your job runs longer
        execution_timeout=pendulum.duration(hours=2),
    )