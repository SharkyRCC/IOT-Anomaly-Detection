# Minimal Dockerfile for IoT Sensor Anomaly Detection - Day 1
# Updated for Python 3.7 compatibility
FROM apache/airflow:2.6.1

# Set environment variables for Day 1
ENV AIRFLOW_HOME=/opt/airflow

# Switch to root for package installation
USER root

# Install system dependencies needed for IoT packages
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*