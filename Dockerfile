# Dockerfile

FROM apache/airflow:2.10.2-python3.9

# Switch to root to install system dependencies if needed
USER root

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt