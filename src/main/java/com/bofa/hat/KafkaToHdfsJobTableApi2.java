package com.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.configuration.Configuration;

public class KafkaToHdfsJob {

    private static String generateKeyFields(String prefix, boolean withDataPrefix) {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= 125; i++) {
            if (withDataPrefix) {
                builder.append("data.").append(prefix).append(i);
            } else {
                builder.append(prefix).append(i).append(" STRING");
            }
            if (i != 125) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }

    public static void main(String[] args) throws Exception {

        // Set up Flink environment with checkpointing and fault tolerance
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(60000); // every 60 seconds
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // Set up Table API environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Define Kafka source table
        tableEnv.executeSql(
            "CREATE TABLE kafka_source (" +
            "type STRING," +
            "operation STRING," +
            "source_table STRING," +
            "payload_sys_id STRING," +
            "`timestamp` BIGINT," +
            "data ROW<" +
            "aliases STRING," +
            "asset STRING," +
            "asset_tag STRING," +
            "assigned_to STRING," +
            "attestation_status STRING," +
            "business_criticality STRING," +
            generateKeyFields("key_", false) +
            ">" +
            ") WITH (" +
            "'connector' = 'kafka'," +
            "'topic' = 'your_topic'," +
            "'properties.bootstrap.servers' = 'your.kafka.broker:9092'," +
            "'properties.group.id' = 'flink-kafka-group'," +
            "'scan.startup.mode' = 'earliest-offset'," +
            "'format' = 'json'," +
            "'json.fail-on-missing-field' = 'false'," +
            "'json.ignore-parse-errors' = 'true'" +
            ")"
        );

        // Define HDFS sink table
        tableEnv.executeSql(
            "CREATE TABLE hdfs_sink (" +
            "type STRING," +
            "operation STRING," +
            "source_table STRING," +
            "payload_sys_id STRING," +
            "`timestamp` BIGINT," +
            "aliases STRING," +
            "asset STRING," +
            "asset_tag STRING," +
            "assigned_to STRING," +
            "attestation_status STRING," +
            "business_criticality STRING," +
            generateKeyFields("key_", false) +
            ") WITH (" +
            "'connector' = 'filesystem'," +
            "'path' = 'hdfs://namenode:8020/flink/output/'," +
            "'format' = 'json'," +
            "'sink.rolling-policy.rollover-interval' = '15 min'" +
            ")"
        );

        // Insert into HDFS sink
        tableEnv.executeSql(
            "INSERT INTO hdfs_sink " +
            "SELECT " +
            "type, operation, source_table, payload_sys_id, `timestamp`, " +
            "data.aliases, data.asset, data.asset_tag, data.assigned_to, data.attestation_status, data.business_criticality, " +
            generateKeyFields("key_", true) +
            " FROM kafka_source"
        );
    }
}
