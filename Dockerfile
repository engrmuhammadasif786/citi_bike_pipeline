FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -e .

COPY dbt_citi_bike/ ./dbt_citi_bike/
COPY dashboard/ ./dashboard/
COPY data/ ./data/
COPY tests/ ./tests/

ENV PYTHONPATH=/app/src
ENV PIPELINE_ENV=local
ENV LOCAL_DATA_DIR=/app/data

EXPOSE 8501

CMD ["sh", "-c", "python -m citi_bike_pipeline.cli run-all && streamlit run dashboard/app.py --server.port=8501 --server.address=0.0.0.0"]
