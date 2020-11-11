package paris.benoit.mob.cluster.loopback.distributed;

import paris.benoit.mob.cluster.MobClusterConfiguration;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaSchemaRegistry {
    static Map<String, Properties> propertiesMap;
    static Map<String, String> schemas = new HashMap<>();

    public static void registerSchema(String tableName, String jsonSchema) {
        schemas.put(tableName, jsonSchema);
    }

    public static void registerConfiguration(MobClusterConfiguration configuration) {
        // Un peu abusif
        propertiesMap = configuration.apps.stream().flatMap(it -> it.sql.stream()).map(it -> it.properties).filter(Objects::nonNull).collect(Collectors.toMap(it -> (String) it.get("mob.table-name"), it -> it));
    }

    static public Map<String, String> getInputSchemas() {
        return propertiesMap.entrySet().stream().filter(it -> it.getValue().get("mob.client-io.category").equals("input" )).filter(it -> null != it.getKey()).collect(Collectors.toMap(Map.Entry::getKey, it -> schemas.get(it.getKey())));
    }

    static  public Map<String, String> getOutputSchemas() {
        return propertiesMap.entrySet().stream().filter(it -> it.getValue().get("mob.client-io.category").equals("output")).filter(it -> null != it.getKey()).collect(Collectors.toMap(Map.Entry::getKey, it -> schemas.get(it.getKey())));
    }


}
