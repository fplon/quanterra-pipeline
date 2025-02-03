# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install the project into `/quanterra-pipeline`
WORKDIR /quanterra-pipeline

# Copy dependency files first for caching
COPY pyproject.toml uv.lock ./

# Create virtual environment first
RUN uv venv .venv --linker copy

# Install dependencies using UV
RUN .venv/bin/uv pip install --no-deps . -v

# Install application
COPY . .
RUN uv pip install --no-deps . -v

# Environment configuration
ENV PATH="/quanterra-pipeline/.venv/bin:$PATH"
ENV PYTHONPATH="/quanterra-pipeline"

# Run the ingestion flow
CMD ["python", "-m", "src.flows.ingestion"] 