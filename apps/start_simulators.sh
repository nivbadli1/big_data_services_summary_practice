#!/usr/bin/env bash
# Starts the two PROVIDED simulators in the background inside the apps container.
#   docker compose exec apps bash /apps/start_simulators.sh
# Their output goes to /apps/logs/*.log (= ./apps/logs on your laptop):
#   docker compose exec apps tail -f /apps/logs/order_app.log
# Stop them:
#   docker compose exec apps bash /apps/stop_simulators.sh
set -euo pipefail
mkdir -p /apps/logs
cd /apps

start() {
  local name=$1 script=$2
  if pgrep -f "$script" > /dev/null; then
    echo "$name is already running"
  else
    nohup python "$script" > "/apps/logs/$name.log" 2>&1 &
    echo "started $name (pid $!) -> /apps/logs/$name.log"
  fi
}

start order_app   /apps/simulators/order_app.py
start courier_sim /apps/simulators/courier_sim.py
sleep 8
echo "--- last lines of order_app.log ---"
tail -n 5 /apps/logs/order_app.log || true
echo "--- last lines of courier_sim.log ---"
tail -n 3 /apps/logs/courier_sim.log || true
