FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Create the directory for the project
WORKDIR /opt/prefect/quanterra-pipeline

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONPATH=/opt/prefect/quanterra-pipeline
ENV PATH="/root/.local/bin:$PATH"
ENV UV_SYSTEM_PYTHON=1

# Copy only necessary files for installation
COPY pyproject.toml ./

# Install dependencies with UV
RUN mkdir -p /root/.cache/uv && \
    uv pip install -e . && \
    rm -rf /root/.cache/uv

# Copy only the source code
COPY src ./src/
