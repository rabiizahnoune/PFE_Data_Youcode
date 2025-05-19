FROM python:3.9-slim

# Set working directory
USER root
RUN apt-get update && apt-get install -y && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY ML/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install MLflow
RUN pip install mlflow

# Create directories for MLflow artifacts and metadata
RUN mkdir -p /app/mlruns /app/mlflow.db

# Set environment variables for MLflow (local tracking)
ENV MLFLOW_TRACKING_URI=sqlite:///mlflow.db
ENV MLFLOW_ARTIFACT_ROOT=/app/mlruns

# Set working directory
WORKDIR /app

# Keep container running
CMD ["tail", "-f", "/dev/null"]