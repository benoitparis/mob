package paris.benoit.mob.cluster.loopback.distributed;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class KafkaClusterConsumer {

    final Properties props;
    final String tableName;
    final ClusterReceiver clusterReceiver;
    final KafkaConsumer<String, String> consumer;

    public KafkaClusterConsumer(Properties props, String tableName, ClusterReceiver clusterReceiver) {
        this.props = Stream.of(props).collect(Properties::new, Map::putAll, Map::putAll);
        // org.apache.kafka.common.serialization.ByteArraySerializer pour du zéro copy?
//        props.put("key.serializer","org.apache.kafka.connect.json.JsonSerializer");
        this.props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        this.props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        this.tableName = tableName;
        this.consumer = new KafkaConsumer<>(this.props);
        this.clusterReceiver = clusterReceiver;
    }

    public void start() {

        // TODO get static local JVM UUID
        consumer.subscribe(Collections.singleton("mobcatalog.ack.send_client"));

        // TODO et comment on stoppe gracefully?
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.records("mobcatalog.ack.send_client").forEach((it) -> {
                    clusterReceiver.receiveMessage(ToClientMessage.fromString(it.value()));
                });
            }
        }).start();



    }
}
