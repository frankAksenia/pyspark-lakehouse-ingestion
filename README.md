# PySpark Lakehouse Data Ingestion Pipeline

Project using PySpark and Delta Lake. The project ingests NYC taxi trip data into a local lakehouse, applies cleaning and data quality rules, and produces analytics-ready KPI tables.

## Data Source

This project uses publicly available NYC Taxi & Limousine Commission trip record data.

## Architecture

Raw parquet -> Bronze Delta -> Silver Delta + rejected records -> Data quality report -> Gold KPI tables

## Tech Stack

- Python
- PySpark
- Delta Lake
- Spark SQL
- Docker
- Makefile
- pytest

## What this project contains

- Data ingestion framework design
- Bronze/silver/gold lakehouse architecture
- ETL/ELT transformations with PySpark
- Schema enforcement and deduplication
- Data quality checks and machine-readable quality reporting
- Analytics-ready KPI tables

## How to run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make download
make run
```

## Outputs

- `lakehouse/bronze/trips`: raw ingested Delta table
- `lakehouse/silver/trips_clean`: cleaned Delta table
- `lakehouse/silver/trips_rejected`: invalid records with rejection reasons
- `lakehouse/gold/daily_revenue`: daily revenue KPI table
- `lakehouse/gold/hourly_demand`: hourly demand KPI table
- `lakehouse/gold/revenue_efficiency`: revenue efficiency KPI table
- `reports/data_quality_report.json`: data quality report
