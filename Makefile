# Short names for commands you'll run often. `make help` lists them.
.PHONY: help setup test docker-test ingest ingest-file profile transform pipeline clean

YEAR ?= 2025
MONTH ?= 11
FILE ?= data/landing/flight_delays_2025_11.csv
FORCE ?=
FORCE_FLAG = $(if $(FORCE),--force,)
DBT = cd dbt && ../.venv/bin/dbt

help:
	@echo "setup        Create .venv and install dependencies"
	@echo "test         Run tests locally"
	@echo "docker-test  Run tests inside Docker"
	@echo "ingest-file  Load a BTS CSV export: make ingest-file FILE=data/landing/<file>.csv [FORCE=1]"
	@echo "ingest       Download one month from BTS: make ingest YEAR=2025 MONTH=11"
	@echo "profile      Check the raw data against the data-model assumptions"
	@echo "transform    Build staging + marts with dbt (seed, run)"
	@echo "pipeline     Load the file, then build the marts"
	@echo "clean        Delete downloaded data and the local warehouse"

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

test:
	.venv/bin/pytest -q

docker-test:
	docker compose run --rm pipeline

ingest-file:
	PYTHONPATH=src .venv/bin/python -m delta_pipeline.ingest --file $(FILE) $(FORCE_FLAG)

ingest:
	PYTHONPATH=src .venv/bin/python -m delta_pipeline.ingest --year $(YEAR) --month $(MONTH) $(FORCE_FLAG)

profile:
	PYTHONPATH=src .venv/bin/python -m delta_pipeline.profile

transform:
	$(DBT) seed --profiles-dir .
	$(DBT) run --profiles-dir .

pipeline: ingest-file transform

clean:
	find data/raw data/warehouse -type f ! -name .gitkeep -delete
	rm -rf dbt/target dbt/logs
