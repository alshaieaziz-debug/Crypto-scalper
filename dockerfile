# Use slim python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make libffi-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install pip dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Expose healthcheck port
EXPOSE 8080

# Run app
CMD ["python", "main.py"]
