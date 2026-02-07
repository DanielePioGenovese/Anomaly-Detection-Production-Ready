# Without Docker
create_datasets:
	uv run --group dataset-creation -m data.prepare_data  
	
# With Docker
hist_service:
	docker compose up hist_ingestion

hist_service_down:
	docker compose down hist_ingestion

