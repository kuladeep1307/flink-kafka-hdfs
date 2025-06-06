flink run -t yarn-per-job \
  -Dsecurity.kerberos.login.keytab=/path/to/flink.keytab \
  -Dsecurity.kerberos.login.principal=flinkuser@YOUR.REALM.COM \
  -c com.example.flink.KafkaJsonToHdfsJob \
  target/flink-kafka-hdfs-1.0.0.jar \
  --kafka.bootstrap.servers broker1:9092 \
  --kafka.topic my-json-topic \
  --hdfs.output.path hdfs:///user/flink/json-output/

Debug in VS Code
Go to the Run and Debug tab → Create launch.json:

{
  "type": "java",
  "name": "KafkaJsonToHdfsJob",
  "request": "launch",
  "mainClass": "com.example.flink.KafkaJsonToHdfsJob",
  "vmArgs": "-Djava.security.auth.login.config=kafka_client_jaas.conf",
  "args": "--kafka.bootstrap.servers=localhost:9092 --kafka.topic=test --hdfs.output.path=hdfs:///user/flink/json-output"
}

*Replace localhost:9092 and topic/path with real values