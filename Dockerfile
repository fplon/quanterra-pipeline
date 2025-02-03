FROM python:3.12-slim

# Prevents Python from writing pyc files
ENV PYTHONDONTWRITEBYTECODE=1
# Keeps Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create a non-privileged user
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir uv && \
    uv pip install --system --no-cache-dir -r requirements.txt

# Copy the project files
COPY src/ ./src/

# Set environment variables
ENV PYTHONPATH=/app

# Switch to non-privileged user
USER appuser

# Command to run the flow
CMD ["python", "src/flows/ingestion.py"] 