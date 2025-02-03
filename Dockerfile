FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy requirements files
COPY requirements.txt .

# Install Python dependencies using uv
RUN uv pip install -r requirements.txt

# Copy the project files
COPY src/ ./src/

# Set environment variables
ENV PYTHONPATH=/app

# Command to run the flow
CMD ["python", "src/flows/ingestion.py"] 