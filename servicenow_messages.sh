#!/bin/bash

# CONFIGURATION
KAFKA_BROKER="localhost:9092"
KAFKA_TOPIC="servicenow_updates"

# Value pools
OPERATIONS=("insert" "update" "delete")
SOURCE_TABLES=("cmdb_ci_service_auto" "cmdb_ci_server" "cmdb_ci_database" "cmdb_ci_web_service")
ATTESTATION_STATUS=("Not yet reviewed" "Approved" "Rejected" "Pending")
BUSINESS_CRITICALITY=("1 - most critical" "2 - highly critical" "3 - moderately critical" "4 - not critical")

# % chance a field will be null (e.g., 30 = 30%)
NULL_CHANCE=30

generate_random_string() {
    tr -dc A-Za-z0-9 </dev/urandom | head -c 16
}

random_choice() {
    local arr=("$@")
    echo "${arr[$RANDOM % ${#arr[@]}]}"
}

maybe_null_string() {
    if (( RANDOM % 100 < NULL_CHANCE )); then
        echo "null"
    else
        echo "\"$(generate_random_string)\""
    fi
}

maybe_null_enum() {
    if (( RANDOM % 100 < NULL_CHANCE )); then
        echo "null"
    else
        echo "\"$(random_choice "$@")\""
    fi
}

generate_data_block() {
    local block=""
    block+="    \"aliases\": null,\n"
    block+="    \"asset\": $(maybe_null_string),\n"
    block+="    \"asset_tag\": $(maybe_null_string),\n"
    block+="    \"assigned_to\": $(maybe_null_string),\n"
    block+="    \"attestation_status\": $(maybe_null_enum "${ATTESTATION_STATUS[@]}"),\n"
    block+="    \"business_criticality\": $(maybe_null_enum "${BUSINESS_CRITICALITY[@]}")"

    for ((i=1; i<=125; i++)); do
        key="key_$i"
        if (( RANDOM % 100 < NULL_CHANCE )); then
            value="null"
        else
            value="\"$(generate_random_string)\""
        fi
        block+=",\n    \"$key\": $value"
    done

    echo -e "$block"
}

trap "echo 'Script terminated.'; exit" SIGINT

i=1
while true; do
    OPERATION=$(random_choice "${OPERATIONS[@]}")
    SOURCE_TABLE=$(random_choice "${SOURCE_TABLES[@]}")
    PAYLOAD_SYS_ID=$(generate_random_string)
    TIMESTAMP=$(date +%s%3N)

    DATA_CONTENT=$(generate_data_block)

    JSON_MSG=$(cat <<EOF
{
  "type": "record",
  "operation": "$OPERATION",
  "source_table": "$SOURCE_TABLE",
  "payload_sys_id": "$PAYLOAD_SYS_ID",
  "timestamp": $TIMESTAMP,
  "data": {
$DATA_CONTENT
  }
}
EOF
)

    echo "[$(date)] Sending message #$i → $KAFKA_TOPIC"
    echo "$JSON_MSG" | kafka-console-producer.sh --broker-list "$KAFKA_BROKER" --topic "$KAFKA_TOPIC" > /dev/null
    ((i++))

    # Control rate
    sleep 1
done
