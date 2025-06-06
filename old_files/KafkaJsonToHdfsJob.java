package com.bofa.hat;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.Che

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaJsonToHdfsJob 
{
    public static void main( String[] args ) throws Exception
    {
        ParameterTool params = ParameterTool.fromArgs(args);

        String kafkaBrokers = params.get("kafka.bootstrap.servers");
        String kafkaTopic = params.get("kafka.topic");
        String groupId = params.get("kafka.group.id", "flink-cdp-json");
        String hdfsPath = params.get("hdfs.output.path");

        Configuration config = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        // Fault tolerance
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(15000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Kafka config with Kerberos (SASL_SSL)
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBrokers);
        kafkaProps.setProperty("group.id", groupId);
        kafkaProps.setProperty("security.protocol", "SASL_SSL");
        kafkaProps.setProperty("ssl.truststore.location", "/path/to/truststore.jks");
        kafkaProps.setProperty("ssl.truststore.password", "changeit");
        kafkaProps.setProperty("ssl.keystore.location", "/path/to/keystore.jks");
        kafkaProps.setProperty("ssl.keystore.password", "changeit");
        kafkaProps.setProperty("ssl.key.password", "changeit");
        kafkaProps.setProperty("sasl.kerberos.service.name", "kafka");
        kafkaProps.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), kafkaProps);
        
        // Deserialize JSON and filter
        ObjectMapper mapper = new ObjectMapper();

        DataStream<String> rawStream = env.addSource(kafkaConsumer);

        DataStream<String> validJsonStream = rawStream
            .filter(json -> {
                try {
                    JsonNode node = mapper.readTree(json);
                    return node.has("eventTime") && node.has("message"); // Example check
                } catch (Exception e) {
                    return false;
                }
            });
        
        // HDFS Sink
        FileSink<String> sink = FileSink
                .forRowFormat(new Path(hdfsPath), new org.apache.flink.core.fs.StringWriter<>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(128 * 1024 * 1024)
                                .build())
                .build();

        validJsonStream.sinkTo(sink);

        env.execute("Kerberos Kafka JSON to HDFS");
    }
}
