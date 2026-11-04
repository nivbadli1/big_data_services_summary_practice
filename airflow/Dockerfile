# Airflow 2.8.4 (same version as the Airflow lesson) + the two clients our DAG needs.
# requirements.txt pins the versions from the official Airflow 2.8.4 constraints file
# (the image runs Python 3.8). Pinning "apache-airflow==2.8.4" stops pip from upgrading Airflow.
# No remote constraints URL: it breaks builds behind HTTPS-inspecting proxies.
FROM apache/airflow:2.8.4

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.8.4" -r /requirements.txt

# Folder for the SQLite metadata DB. Created here (as the airflow user, group 0 writable) so the
# named volume mounted on it inherits these permissions.
RUN mkdir -p /opt/airflow/db && chmod -R g+rwX /opt/airflow/db
