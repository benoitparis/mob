package paris.benoit.mob.cluster.loopback.distributed;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterSender;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

public class KafkaClusterSender implements ClusterSender {

    final Properties props;
    final String tableName;
    final KafkaProducer<String, String> producer;

    public KafkaClusterSender(Properties props, String tableName) {
        this.props = Stream.of(props).collect(Properties::new, Map::putAll, Map::putAll);
        // org.apache.kafka.common.serialization.ByteArraySerializer pour du zéro copy?
//        props.put("key.serializer","org.apache.kafka.connect.json.JsonSerializer");
        this.props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        this.props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        this.tableName = tableName;
        this.producer = new KafkaProducer(this.props);
    }

    @Override
    public void sendMessage(ToServerMessage message) throws Exception {

        ProducerRecord<String, String> msg = new ProducerRecord<>(tableName, message.toJsonString());
        producer.send(msg);
        System.out.println("sent : " + msg);
    }

    @Override
    public ToServerMessage receive() throws Exception {
        return null;
    }


}
