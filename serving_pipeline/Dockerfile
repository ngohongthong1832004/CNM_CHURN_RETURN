# Use Python 3.10 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the serving_pipeline_pipeline_pipeline module (parent directory for imports)
COPY load_model.py .
COPY pre_processing.py .
COPY monitoring.py .

# Copy the API application
COPY api/ ./api/

# Create necessary directories
RUN mkdir -p ./api/data ./api/reports

# Expose the port
EXPOSE 8000

WORKDIR /app/api

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV MODEL_URI=${MODEL_URI}
ENV MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')" || exit 1

# Run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
