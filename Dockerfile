FROM apache/airflow:2.10.2

# Switch to airflow user for installing Python packages
USER airflow

# Copy requirements.txt and install the packages as airflow user
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
