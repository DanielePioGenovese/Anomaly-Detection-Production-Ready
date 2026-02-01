# Declare 'data' as a phony target so it runs even if the folder exists
.PHONY: data

data:
	uv run python -m data.prepare_data

# Variables
PYTHON = uv run
DOCKER = docker compose

# Phony targets (not real files)
.PHONY: help up down consumer producer run-all clean

# --- 1. Help Menu ---
help:
	@echo "Available commands:"
	@echo "  make up       -> Start Kafka infrastructure (Docker)"
	@echo "  make down     -> Stop Kafka infrastructure"
	@echo "  make consumer -> Run the Consumer (Anomaly Detector) in this terminal"
	@echo "  make producer -> Run the Producer (Sensor Simulation) in this terminal"
	@echo "  make run-all  -> 🚀 THE MAGIC BUTTON: Starts Kafka + Opens Consumer & Producer in NEW windows"

# --- 2. Infrastructure ---
up:
	@echo "Starting Kafka..."
	$(DOCKER) up -d
	@echo "✅ Kafka is running. UI at http://localhost:8080"

down:
	$(DOCKER) down

# --- 3. Single Components (Manual Mode) ---
consumer:
	$(PYTHON) -m src.app

producer:
	$(PYTHON) -m src.queue.producer

# --- 4. Maintenance ---
clean:
	@echo "Cleaning pycache..."
	@if exist "__pycache__" rmdir /s /q "__pycache__"