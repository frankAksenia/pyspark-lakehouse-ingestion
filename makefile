.PHONY: install download run test lint clean

install:	
	pip install -r requirements.txt

download:	
	python -m src.jobs.download_data

run:	
	python src/jobs/ingest_bronze.py	
	python src/jobs/transform_silver.py	
	python src/jobs/data_quality.py	
	python src/jobs/build_gold.py

test:	
	pytest -q

lint:	
	ruff check src tests

clean:	
	rm -rf lakehouse reports/*.json reports/*.csv .pytest_cache .ruff_cache