FROM python:3.12-slim-bookworm

# Keep Python logs visible immediately in `docker compose logs`.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first for better Docker layer caching.
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

# Copy application code.
COPY . .

EXPOSE 8765

# Run the trading service in the foreground so Docker can supervise it.
CMD ["python", "app.py"]
