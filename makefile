.PHONY: help create_datasets data_engineering run_all apply_feature_store service_feature_store run_feature_store clean

# ----------------------------------------------
# Display help message
# ----------------------------------------------
help:
	@echo "=============================================="
	@echo "DATA PIPELINE & FEATURE STORE MAKEFILE"
	@echo "=============================================="
	@echo "1) create_datasets     	 : Build/Run dataset & engineering"
	@echo "2) run_all            	 : Dataset creation + feast"
	@echo "3) apply_feature_store 	 : Create Parquet files & Apply Feast Schema"
	@echo "3) service_feature_store  : Run Feast service"
	@echo "4) run_feature_store   	 : Full Stack (Redis + Apply + Serve)"
	@echo "5) clean               	 : Stop all containers"
	@echo "=============================================="

# ----------------------------------------------
# Create datasets and run data engineering
# ----------------------------------------------
create_datasets: data_engineering

data_engineering:
	@echo "Syncing environment..."
	uv sync
	@echo "Building and running data engineering container..."
	docker compose up --build data_engineering

# ----------------------------------------------
# Apply feature store: create offline files + apply Feast schema
# ----------------------------------------------
apply_feature_store:
	@echo "Building local environment and creating offline storage files..."
	uv sync
	uv run python -m utils.create_offline_files
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
# Clean all containers and volumes
# ----------------------------------------------
clean:
	@echo "Stopping all containers and removing volumes..."
	docker compose down
	@echo "Environment cleaned."

# ----------------------------------------------
# Run everything: create datasets + feature store
# ----------------------------------------------
run_all: create_datasets run_feature_store
