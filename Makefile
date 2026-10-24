# Short names for commands you'll run often. `make help` lists them.
.PHONY: help setup test docker-test clean

help:
	@echo "setup        Create .venv and install dependencies"
	@echo "test         Run tests locally"
	@echo "docker-test  Run tests inside Docker"
	@echo "clean        Delete downloaded data and the local warehouse"

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

test:
	.venv/bin/pytest -q

docker-test:
	docker compose run --rm pipeline

clean:
	find data/raw data/warehouse -type f ! -name .gitkeep -delete
