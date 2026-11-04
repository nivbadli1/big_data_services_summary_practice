#!/usr/bin/env bash
# Stops the simulators started by start_simulators.sh
#   docker compose exec apps bash /apps/stop_simulators.sh
pkill -f /apps/simulators/order_app.py && echo "stopped order_app" || echo "order_app was not running"
pkill -f /apps/simulators/courier_sim.py && echo "stopped courier_sim" || echo "courier_sim was not running"
