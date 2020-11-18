package paris.benoit.mob.cluster.loopback.distributed;

import paris.benoit.mob.cluster.MobClusterConfiguration;

import java.util.*;
import java.util.stream.Collectors;

// TODO faire tout ça en Flink SQL qui lit Kafka?
//   [tous ces stream() c'est du JOIN en SQL, et qui est public à toutes les instances
//   matérializer les joins et envoyer des updates c'est pas le taf de ça?
//   faudra mettre les bons offsets (sinon on recompute)
//     ou bien dire: on deploy à partir de là (WHERE > timestamp/event/deployment_id)
//   dans un catalog / app qui s'appelerait: admin et où les lectures sont restreintes
// TODO et on aurait une admin en react, avec toutes ces infos

public class KafkaSchemaRegistry {

    public static final String MOB_CLIENT_IO_CATEGORY = "mob.client-io.category";
    public static final String MOB_TABLE_NAME = "mob.table-name";
    public static final String MOB_CLUSTER_IO_TYPE = "mob.cluster-io.type";
    public static final String MOB_CLUSTER_IO_FLOW = "mob.cluster-io.flow";

    static Map<String, Properties> propertiesMap;
    static final Map<String, String> schemas = new HashMap<>();

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
        System.out.println("getOutputSchemas");
        return getSchemas("output");
    }

    static public Map<String, String> getSchemas(String category) {
        // TODO Read from somewhere? rest?
        //   posté sur un topic kafka, interrogé par le front?
        //     genre un front qui interroge le backend comme si il était un client,
        //       et qui reçoit comment gérer ses clients par ce topic
        //       une HashMap qui se fait updater?
        //         client-io.table.mobcatalog.adder.sendclient.table.status = available
        //         client-io.table.mobcatalog.adder.sendclient.table.metadata = ..
        //       une liste d'ordres?
        //         front-command = drain-clients-ask-relogin
        //           avec des events cloud pour kill si < cpu?

        try {
            return propertiesMap.entrySet().stream()
                    .filter(it -> !"js-engine".equals(it.getValue().get(MOB_CLUSTER_IO_TYPE))) // TODO faire par defaut client? ou bien demander?
                    .filter(it -> it.getValue().get(MOB_CLIENT_IO_CATEGORY).equals(category))
                    .filter(it -> null != it.getKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, it -> schemas.get(it.getKey())));
        } catch (NullPointerException e) {
            // TODO cleanup that
            //   no info?
            //   wrong info -> make it generated (you already have the app name and catalog)
            throw new RuntimeException("Your kafka tables probably did not contain sufficient metadata or did not contain the right one", e);
        }
    }

    public static Map<String, Properties> getJsEngineConfiguration() {
        // strings pour le moment
        return propertiesMap.entrySet().stream()
                .filter(it -> "js-engine".equals(it.getValue().get(MOB_CLUSTER_IO_TYPE)))
                .collect(Collectors.toMap(it -> (String) it.getValue().get(MOB_CLUSTER_IO_FLOW), Map.Entry::getValue));
    }
}
