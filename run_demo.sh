#!/bin/bash

# Exit on any error
set -e

DURATION=${1:-180}

echo "EBS Pub/Sub System - End-to-end & Evaluation"
echo "Duration: $DURATION seconds"

echo -e "\n[1/5] Setting up Python virtual environment..."
cd pubsub

if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=python3
else
    PYTHON_CMD=python
fi

$PYTHON_CMD -m venv venv

if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi

python -m pip install --upgrade pip setuptools wheel
python -m pip install pika protobuf grpcio-tools fastapi uvicorn requests

echo -e "\n[2/5] Compiling Protobuf schemas..."
python -m grpc_tools.protoc -I. --python_out=. proto/messages.proto

echo -e "\n[3/5] Starting RabbitMQ..."
docker compose up -d
echo "Waiting 10 seconds for RabbitMQ to be fully ready..."
sleep 20

echo -e "\n[4/5] Running Evaluation..."
# The evaluation script will:
# - Look for the pre-generated evaluation data sets in pubsub/evaluation/data_100 and data_25
# - Spin up Coordinator, Brokers, Subscribers, Publishers.
# - Run a feed for each scenario (3 minutes by default).
# - Generate Evaluation_Report.md.
cd ..

if [ -f "pubsub/venv/Scripts/activate" ]; then
    source pubsub/venv/Scripts/activate
else
    source pubsub/venv/bin/activate
fi

python pubsub/evaluation/run_evaluation.py --duration $DURATION

# cleanup
echo -e "\n[5/5] Stopping RabbitMQ..."
cd pubsub
docker compose down

echo "Finished successfully!"
echo "Please check Evaluation_Report.md for results."
