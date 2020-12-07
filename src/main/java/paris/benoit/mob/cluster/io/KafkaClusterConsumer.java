package paris.benoit.mob.cluster.io;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

import java.time.Duration;
import java.util.Collections;

public class KafkaClusterConsumer {

    final String tableName;
    final ClusterReceiver clusterReceiver;
    final KafkaConsumer<String, String> consumer;

    public KafkaClusterConsumer(String tableName, ClusterReceiver clusterReceiver) {
        this.tableName = tableName;
        this.clusterReceiver = clusterReceiver;
        this.consumer = new KafkaConsumer<>(KafkaGlobals.getConnectOptions("client"));
//        KafkaAdminClient.create().
//        new NewTopic.configs
    }

    public void start() {
        // TODO get static local JVM UUID
        consumer.subscribe(Collections.singleton(tableName));

        // TODO et comment on stoppe gracefully?
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.records(tableName).forEach((it) -> clusterReceiver.receiveMessage(ToClientMessage.fromString(it.value(), tableName)));
            }
        }).start();

    }
}
