# The light "apps" container: simulators, your consumer, the chaos script and the tests.
FROM python:3.11-slim

# procps = ps / pgrep / pkill (used by start_simulators.sh and stop_simulators.sh)
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /apps
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
