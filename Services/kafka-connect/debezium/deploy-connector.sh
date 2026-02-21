#!/bin/bash
set -e

CONNECTOR_NAME="postgres-source-connector"
KAFKA_CONNECT_URL="http://localhost:8083"

echo "Waiting for Kafka Connect to be ready..."
while ! curl -sf $KAFKA_CONNECT_URL > /dev/null; do
    sleep 5
done

echo "Kafka Connect is ready. Deploying connector..."

# Check if connector already exists
if curl -sf $KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME > /dev/null 2>&1; then
    echo "Connector already exists. Checking status..."
    STATUS=$(curl -s $KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/status | jq -r '.connector.state')
    if [ "$STATUS" != "RUNNING" ]; then
        echo "Connector not running. Restarting..."
        curl -X POST $KAFKA_CONNECT_URL/connectors/$CONNECTOR_NAME/restart
    else
        echo "Connector is already running."
    fi
else
    echo "Creating connector..."
    curl -X POST $KAFKA_CONNECT_URL/connectors \
        -H "Content-Type: application/json" \
        -d @/kafka-connect/postgres-connector.json
    
    sleep 5
    echo "Connector deployed successfully."
fi

echo "Connector deployment complete."