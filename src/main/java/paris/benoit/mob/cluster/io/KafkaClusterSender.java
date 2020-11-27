package paris.benoit.mob.cluster.io;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterSender;

public class KafkaClusterSender implements ClusterSender {

    final String tableName;
    final KafkaProducer<String, String> producer;

    public KafkaClusterSender(String tableName) {
        this.tableName = tableName;
        this.producer = new KafkaProducer<>(KafkaGlobals.getConnectOptionsForGroupId("client"));
    }

    @Override
    public void sendMessage(ToServerMessage message) {
        ProducerRecord<String, String> msg = new ProducerRecord<>(tableName, message.toJsonString());
        producer.send(msg);
    }

}
