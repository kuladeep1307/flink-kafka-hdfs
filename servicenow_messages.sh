#!/bin/bash

# CONFIGURATION
KAFKA_BROKER="localhost:9092"
KAFKA_TOPIC="servicenow_updates"

# Possible values for random fields
OPERATIONS=("insert" "update" "delete")
SOURCE_TABLES=("cmdb_ci_service_auto" "cmdb_ci_server" "cmdb_ci_database" "cmdb_ci_web_service")
ATTESTATION_STATUS=("Not yet reviewed" "Approved" "Rejected" "Pending")
BUSINESS_CRITICALITY=("1 - most critical" "2 - highly critical" "3 - moderately critical" "4 - not critical")

# Generate random string
generate_random_string() {
    tr -dc A-Za-z0-9 </dev/urandom | head -c 16
}

# Random choice from array
random_choice() {
    local arr=("$@")
    echo "${arr[$RANDOM % ${#arr[@]}]}"
}

# Generate data block with 125 random key-values
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

# Graceful exit on Ctrl+C
trap "echo 'Script terminated.'; exit" SIGINT

# Infinite loop
i=1
while true; do
    OPERATION=$(random_choice "${OPERATIONS[@]}")
    SOURCE_TABLE=$(random_choice "${SOURCE_TABLES[@]}")
    PAYLOAD_SYS_ID=$(generate_random_string)
    TIMESTAMP=$(date +%s%3N)
    ASSET=$(generate_random_string)
    ASSET_TAG=$(generate_random_string)
    ASSIGNED_TO=$(generate_random_string)
    ATTESTATION=$(random_choice "${ATTESTATION_STATUS[@]}")
    CRITICALITY=$(random_choice "${BUSINESS_CRITICALITY[@]}")

    DATA_BLOCK=$(generate_data_block)

    JSON_MSG=$(cat <<EOF
{
  "type": "record",
  "operation": "$OPERATION",
  "source_table": "$SOURCE_TABLE",
  "payload_sys_id": "$PAYLOAD_SYS_ID",
  "timestamp": $TIMESTAMP,
  "data": {
    "aliases": null,
    "asset": "$ASSET",
    "asset_tag": "$ASSET_TAG",
    "assigned_to": "$ASSIGNED_TO",
    "attestation_status": "$ATTESTATION",
    "business_criticality": "$CRITICALITY",
$(echo "$DATA_BLOCK" | tail -n +2)
  }
}
EOF
)

    echo "[$(date)] Sending message #$i → $KAFKA_TOPIC (op=$OPERATION, table=$SOURCE_TABLE)..."
    echo "$JSON_MSG" | kafka-console-producer.sh --broker-list "$KAFKA_BROKER" --topic "$KAFKA_TOPIC" > /dev/null
    ((i++))

    # Optional: Control rate
    sleep 1
done
