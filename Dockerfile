# A small, reproducible Python environment for the pipeline.
# CI builds this image and runs the full test suite inside it (see .github/workflows/ci.yml).
FROM python:3.12-slim

WORKDIR /app

# Install dependencies first so Docker can cache this layer:
# changing code won't trigger a full reinstall.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml .
COPY src/ src/
COPY dbt/ dbt/
COPY scripts/ scripts/
COPY tests/ tests/

ENV PYTHONPATH=/app/src

CMD ["pytest", "-q"]
