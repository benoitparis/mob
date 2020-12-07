package paris.benoit.mob.cluster.io;

import java.util.HashMap;
import java.util.Map;

public class KafkaGlobals {

    private static final Map<String, String> FLINK_KAFKA_TABLE_OPTIONS = new HashMap<>();
    static {
        FLINK_KAFKA_TABLE_OPTIONS.put("connector", "kafka");
        FLINK_KAFKA_TABLE_OPTIONS.put("properties.bootstrap.servers", "localhost:9092");
        FLINK_KAFKA_TABLE_OPTIONS.put("value.format", "json");
    }
    private static final Map<String, String> FLINK_KAFKA_UPSERT_TABLE_OPTIONS = new HashMap<>();
    static {
        FLINK_KAFKA_UPSERT_TABLE_OPTIONS.put("connector", "upsert-kafka");
        FLINK_KAFKA_UPSERT_TABLE_OPTIONS.put("properties.bootstrap.servers", "localhost:9092");
        FLINK_KAFKA_UPSERT_TABLE_OPTIONS.put("key.format", "json");
        FLINK_KAFKA_UPSERT_TABLE_OPTIONS.put("value.format", "json");
    }
    public static Map<String, String> getTableOptions(String tableName, boolean upsert) {
        HashMap<String, String> result = new HashMap<>();
        if (upsert) {
            result.putAll(FLINK_KAFKA_UPSERT_TABLE_OPTIONS);
        } else {
            result.putAll(FLINK_KAFKA_TABLE_OPTIONS);
        }
        result.put("topic", tableName);
        return result;
    }


    private static final Map<String, Object> KAFKA_CONNECT_OPTIONS = new HashMap<>();
    static {
        // TODO passer à avro / protobuf, etc. avec org.apache.kafka.common.serialization.ByteArraySerializer pour du zéro copy?
        KAFKA_CONNECT_OPTIONS.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
        KAFKA_CONNECT_OPTIONS.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
        KAFKA_CONNECT_OPTIONS.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        KAFKA_CONNECT_OPTIONS.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        KAFKA_CONNECT_OPTIONS.put("bootstrap.servers", "localhost:9092");
    }
    public static Map<String, Object> getConnectOptions(String groupId) {
        HashMap<String, Object> result = new HashMap<>(KafkaGlobals.KAFKA_CONNECT_OPTIONS);
        result.put("group.id", groupId);
        return result;
    }

}
