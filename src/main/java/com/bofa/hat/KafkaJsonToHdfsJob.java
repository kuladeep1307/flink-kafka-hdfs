package com.bofa.hat;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class KafkaJsonToHdfsJob {
    public static void main(String[] args) throws Exception {
        // Parse parameters from CLI args
        final ParameterTool params = ParameterTool.fromArgs(args);

        String kafkaBootstrapServers = params.getRequired("kafka-bootstrap-servers");
        String kafkaTopic = params.getRequired("kafka-topic");
        String kafkaGroupId = params.get("kafka-group-id", "flink-group");
        // String outputPath = params.getRequired("output-path");
        Path outputPath = new Path(params.getRequired("output-path"));
        String kerberosServiceName = params.get("kerberos-service-name", "kafka");

        // Setup Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        env.getCheckpointConfig().setCheckpointTimeout(60_000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Build KafkaSource with SASL_SSL Kerberos configs
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId(kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "GSSAPI")
                .setProperty("sasl.kerberos.service.name", kerberosServiceName)
                .build();

        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        // Define HDFS FileSink with rolling policy
        
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(outputPath, new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
            .withMaxPartSize(1024 * 1024 * 128)
            .build())
        .build();

        kafkaStream.addSink(sink);

        env.execute("Kafka JSON to HDFS Flink Job");
    }
}
