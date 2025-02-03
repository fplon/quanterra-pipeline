# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install the project into `/quanterra-pipeline`
WORKDIR /quanterra-pipeline

# Copy dependency files first for caching
COPY pyproject.toml uv.lock ./

# Install dependencies using UV
RUN uv pip install --no-deps . -v && \
    uv venv --seed --linker copy

# Install application
COPY . .
RUN uv pip install --no-deps . -v

# Environment configuration
ENV PATH="/quanterra-pipeline/.venv/bin:$PATH"
ENV PYTHONPATH="/quanterra-pipeline"

# Run the ingestion flow
CMD ["python", "-m", "src.flows.ingestion"] 