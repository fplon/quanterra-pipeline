FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /opt/prefect/quanterra-pipeline

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
# ENV PYTHONPATH=/opt/prefect/quanterra-pipeline
ENV PATH="/root/.local/bin:$PATH"
ENV UV_SYSTEM_PYTHON=1

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies with UV (using traditional Docker layer caching)
RUN mkdir -p /root/.cache/uv && \
    uv pip install -e .

# Copy the rest of the application
COPY . .
