# Airflow 2.8.4 (same version as the Airflow lesson) + the two clients our DAG needs.
# The official constraints file pins versions that are known to work with Airflow 2.8.4
# (the image runs Python 3.8). pandas and the Elasticsearch client are already in the image;
# installing them with the constraints keeps the exact tested versions.
FROM apache/airflow:2.8.4

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.8.4" -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.8.txt"

# Folder for the SQLite metadata DB. Created here (as the airflow user, group 0 writable) so the
# named volume mounted on it inherits these permissions.
RUN mkdir -p /opt/airflow/db && chmod -R g+rwX /opt/airflow/db
