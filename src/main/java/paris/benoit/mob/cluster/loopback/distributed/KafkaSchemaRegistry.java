package paris.benoit.mob.cluster.loopback.distributed;

import paris.benoit.mob.cluster.MobClusterConfiguration;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaSchemaRegistry {

    public static final String MOB_CLIENT_IO_CATEGORY = "mob.client-io.category";
    public static final String MOB_TABLE_NAME = "mob.table-name";

    static Map<String, Properties> propertiesMap;
    static Map<String, String> schemas = new HashMap<>();

    public static void registerSchema(String tableName, String jsonSchema) {
        // TODO Write to kafka, with properties, with category parsed, with client_accessible, schema, etc ? "TableRegistry"
        schemas.put(tableName, jsonSchema);
    }

    public static void registerConfiguration(MobClusterConfiguration configuration) {
        propertiesMap = configuration.apps.stream()
                .flatMap(it -> it.sql.stream())
                .map(it -> it.properties)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(it -> (String) it.get(MOB_TABLE_NAME), it -> it));
    }

    static public Map<String, String> getInputSchemas() {
        return getSchemas("input");
    }

    static  public Map<String, String> getOutputSchemas() {
        return getSchemas("output");
    }

    static  public Map<String, String> getSchemas(String category) {
        // TODO Read from kafka?
        return propertiesMap.entrySet().stream()
                .filter(it -> it.getValue().get(MOB_CLIENT_IO_CATEGORY).equals(category))
                .filter(it -> null != it.getKey())
                .collect(Collectors.toMap(Map.Entry::getKey, it -> schemas.get(it.getKey())));
    }

}
