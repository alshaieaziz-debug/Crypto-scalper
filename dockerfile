# Use a small Python image
FROM python:3.11-slim

# Set envs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Riyadh

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata curl \
 && rm -rf /var/lib/apt/lists/*

# Set work dir
WORKDIR /app

# Copy your bot file into the image
COPY app.py /app/app.py

# Install Python deps (match what your app uses)
RUN pip install --upgrade pip && \
    pip install aiohttp uvloop pydantic pydantic-settings structlog tzdata orjson

# Expose port for health endpoint
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=5 \
  CMD curl -fsS "http://127.0.0.1:${PORT:-8080}/healthz" || exit 1

# Run the bot
CMD ["python", "app.py"]