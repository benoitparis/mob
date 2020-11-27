package paris.benoit.mob.cluster.io;

import java.util.HashMap;
import java.util.Map;

public class KafkaGlobals {

    private static final Map<String, String> FLINK_TABLE_OPTIONS = new HashMap<>();
    static {
        // TODO get reference options, have constants (get them from lib?)
        FLINK_TABLE_OPTIONS.put("connector.type", "kafka");
        FLINK_TABLE_OPTIONS.put("connector.version", "universal");
        FLINK_TABLE_OPTIONS.put("connector.property-version", "1");
        FLINK_TABLE_OPTIONS.put("connector.properties.bootstrap.servers", "localhost:9092");
        FLINK_TABLE_OPTIONS.put("connector.properties.zookeeper.connect", "localhost:2181");
        FLINK_TABLE_OPTIONS.put("format.type", "json");
    }
    public static Map<String, String> getTableOptionsForTopic(String topic) {
        HashMap<String, String> result = new HashMap<>(KafkaGlobals.FLINK_TABLE_OPTIONS);
        result.put("connector.topic", topic);
        return result;
    }

    private static final Map<String, Object> KAFKA_CONNECT_OPTIONS = new HashMap<>();
    static {
        // TODO passer à avro / protobuf, etc. avec org.apache.kafka.common.serialization.ByteArraySerializer pour du zéro copy?
        KAFKA_CONNECT_OPTIONS.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KAFKA_CONNECT_OPTIONS.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KAFKA_CONNECT_OPTIONS.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KAFKA_CONNECT_OPTIONS.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KAFKA_CONNECT_OPTIONS.put("bootstrap.servers", "localhost:9092");
    }
    public static Map<String, Object> getConnectOptionsForGroupId(String groupId) {
        HashMap<String, Object> result = new HashMap<>(KafkaGlobals.KAFKA_CONNECT_OPTIONS);
        result.put("group.id", groupId);
        return result;
    }

}
