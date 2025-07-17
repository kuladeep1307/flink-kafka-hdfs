#!/bin/bash

# CONFIGURATION
KAFKA_BROKER="localhost:9092"
KAFKA_TOPIC="servicenow_updates"

generate_random_string() {
    tr -dc A-Za-z0-9 </dev/urandom | head -c 16
}

generate_data_block() {
    echo "{"
    for ((i=1; i<=125; i++)); do
        key="key_$i"
        value=$(generate_random_string)
        if (( i < 125 )); then
            echo "  \"$key\": \"$value\","
        else
            echo "  \"$key\": \"$value\""
        fi
    done
    echo "}"
}

# Trap Ctrl+C to gracefully exit
trap "echo 'Script terminated.'; exit" SIGINT

# Infinite loop
i=1
while true; do
    PAYLOAD_SYS_ID=$(generate_random_string)
    TIMESTAMP=$(date +%s%3N)

    JSON_MSG=$(cat <<EOF
{
  "type": "record",
  "operation": "update",
  "source_table": "cmdb_ci_service_auto",
  "payload_sys_id": "$PAYLOAD_SYS_ID",
  "timestamp": $TIMESTAMP,
  "data": $(generate_data_block)
}
EOF
)

    echo "[$(date)] Sending message #$i to Kafka topic '$KAFKA_TOPIC'..."
    echo "$JSON_MSG" | kafka-console-producer.sh --broker-list "$KAFKA_BROKER" --topic "$KAFKA_TOPIC" > /dev/null
    ((i++))

    # Optional: Add a delay (e.g., 1 sec) to control the message rate
    sleep 1
done
