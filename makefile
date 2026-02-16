create_datasets_2:
	uv sync
	uv run -m data.prepare_data --group dataset-creation-2
	
hist_ingestion:
	uv sync
	docker compose up --build hist_ingestion

create_datasets:
	uv sync
	docker compose up --build create_datasets

run_all:
	uv sync
	docker compose up --build create_datasets hist_ingestion
