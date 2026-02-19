.PHONY: help create_datasets data_engineering final_datasets apply_feature_store service_feature_store run_feature_store clean run_all \
        build_mlflow build_training build_all \
        mlflow_up training_up all_up \
        mlflow_down training_down all_down

# ----------------------------------------------
# Display help message
# ----------------------------------------------
help:
	@echo "=============================================="
	@echo "DATA PIPELINE & FEATURE STORE MAKEFILE"
	@echo "=============================================="
	@echo "1) create_datasets         : Build/Run dataset & engineering"
	@echo "2) data engineering        : Run data_engineering"
	@echo "3) final_datasets          : Run create_datasets + data_engineering"
	@echo "4) apply_feature_store     : Create Parquet files & Apply Feast Schema"
	@echo "5) service_feature_store   : Run Feast service"
	@echo "6) run_feature_store       : Full Stack (Redis + Apply + Serve)"
	@echo "7) run_all                 : Dataset creation + feature store"
	@echo "8) all_up                  : Start MLflow and Training Pipeline"
	@echo "9) all_down                : Stop MLflow and Training Pipeline"
	@echo "10) clean                  : Stop all containers and remove volumes"
	@echo "=============================================="

# ----------------------------------------------
# Create datasets and run data engineering
# ----------------------------------------------
create_datasets: 
	@echo "Syncing environment..."
	uv sync
	@echo "Building datasets..."
	docker compose up --build create_datasets

data_engineering:
	@echo "Syncing environment..."
	uv sync
	@echo "Building and running data engineering container..."
	docker compose up --build data_engineering

final_datasets: create_datasets data_engineering

# ----------------------------------------------
# Apply feature store: create offline files + apply Feast schema
# ----------------------------------------------
apply_feature_store:
	@echo "Building local environment and creating offline storage files..."
	uv sync
	uv run -m --group data-offline utils.create_offline_files
	@echo "Starting infra and applying Feast registry..."
	docker compose up -d redis redpanda
	docker compose up --build --abort-on-container-exit feature_store_apply

# ----------------------------------------------
# Run Feast server
# ----------------------------------------------
service_feature_store:
	@echo "Starting Feast Server..."
	docker compose up --build -d feature_store_service
	@echo "Feast Server is running. Check health at http://localhost:6566/health"
	docker compose logs -f feature_store_service

# ----------------------------------------------
# Full Feature Store workflow: apply + serve
# ----------------------------------------------
run_feature_store: apply_feature_store service_feature_store

# ----------------------------------------------
# Run everything: create datasets + feature store
# ----------------------------------------------
run_all: final_datasets run_feature_store

# ----------------------------------------------
# Build ML and Training Services
# ----------------------------------------------
build_mlflow:
	@echo "Building MLflow..."
	docker compose up --build mlflow

build_training:
	@echo "Building Training Pipeline..."
	docker compose up --build training_pipeline

build_all: build_mlflow build_training
	@echo "All ML and Training services built."

# ----------------------------------------------
# Start ML and Training Services (UP)
# ----------------------------------------------
mlflow_up:
	@echo "Starting MLflow..."
	docker compose up -d mlflow
	@echo "Waiting for MLflow to be ready..."
	sleep 10

training_up:
	@echo "Starting Training Pipeline..."
	docker compose up -d training_pipeline

all_up: mlflow_up training_up
	@echo "Started MLflow and Training Pipeline."

# ----------------------------------------------
# Stop ML and Training Services (DOWN)
# ----------------------------------------------
mlflow_down:
	@echo "Stopping MLflow..."
	docker compose stop mlflow

training_down:
	@echo "Stopping Training Pipeline..."
	docker compose stop training_pipeline

all_down: mlflow_down training_down
	@echo "All ML and Training services stopped."

# ----------------------------------------------
# Clean all containers and volumes
# ----------------------------------------------
clean:
	@echo "Stopping all containers and removing volumes..."
	docker compose down
	@echo "Environment cleaned."