# ---------------------------- FINAL DEBUG

init:
	sudo mkdir -p .redpanda_storage .logs
	sudo chmod -R 777 .redpanda_storage .logs

full_datasets:
	docker compose up --build \
		create_datasets \
		data_engineering \
		create_offline_files

full_architecture:
	@echo "  🚀 Full Architecture (WITH Airflow)"
	docker compose up \
		redis \
		redpanda \
		redpanda-console \
		redpanda-init \
		mlflow \
		qdrant \
		mongodb \
		postgres_airflow \
		airflow-init \
		airflow-webserver \
		airflow-scheduler \
		feature_store_apply \
		feature_store_service \
		streaming_service \
		inference_service \
		ingestion_rag \
		mcp_server \
		vllm \
		langchain_service \
		if_anomaly 

first_training:
	docker compose up --build --abort-on-container-exit training_service 

cold_start:
	docker compose up --build cold_start

full_data_flow:
	docker compose up \
		producer_service


# sudo chown -R 101:101 ./redpanda_storage

clean_data:

	docker compose stop inference_service streaming_service || true

	sudo rm -rf data/offline/streaming_backfill
	sudo rm -rf data/entity_df/telemetry_data
	sudo rm -rf /tmp/quix_state/
	sudo rm -rf /inference_service/state/

	docker compose up create_offline_files
	docker exec -it redpanda_broker rpk topic delete telemetry-data predictions || true
	docker exec -it redpanda_broker rpk topic create telemetry-data -p 1 -r 1
	docker exec -it redpanda_broker rpk topic create predictions -p 1 -r 1

	docker compose start inference_service streaming_service
	