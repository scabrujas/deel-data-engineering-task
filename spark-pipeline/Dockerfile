FROM python:3.11-slim

WORKDIR /app

# Install Java for Spark and PostgreSQL client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    wget \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY src/ /app/src/
COPY run_analytics.sh /app/run_analytics.sh
COPY query_views.sh /app/query_views.sh
RUN chmod +x /app/run_analytics.sh /app/query_views.sh

# Default command
CMD ["python", "-m", "src.main"]
