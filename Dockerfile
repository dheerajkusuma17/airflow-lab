# Start from the exact Airflow version the Compose file uses
FROM apache/airflow:3.4.0

# Copy our package list into the image
COPY requirements.txt /requirements.txt

# Install our packages once, at build time.
# Naming apache-airflow with its own version stops pip from accidentally upgrading Airflow itself.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt