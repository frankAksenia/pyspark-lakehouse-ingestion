.PHONY: install download run test lint clean

install:	
	pip install -r requirements.txt

download:	
	python -m src.jobs.download_data
	
run:
	python -m src.jobs.ingest_bronze
	python -m src.jobs.transform_silver	
	python -m src.jobs.data_quality
	python -m src.jobs.build_gold

test:	
	pytest -q

lint:	
	ruff check src tests

clean:	
	rm -rf lakehouse reports/*.json reports/*.csv .pytest_cache .ruff_cache