# Dockerfile for E-commerce Event Generator
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY azure_blob_writer.py .
COPY event_generator_postgres_azure.py .

# Make script executable
RUN chmod +x event_generator_postgres_azure.py

# Run the event generator
CMD ["python", "-u", "event_generator_postgres_azure.py"]
