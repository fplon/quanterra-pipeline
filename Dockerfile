FROM python:3.12-slim

WORKDIR /opt/prefect/quanterra-pipeline

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY pyproject.toml ./

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Copy the rest of the application
COPY . .
