package paris.benoit.mob.cluster.io;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

// TODO faire tout ça en Flink SQL qui lit Kafka?
//   [tous ces stream() c'est du JOIN en SQL, et qui est public à toutes les instances
//   matérializer les joins et envoyer des updates c'est pas le taf de ça?
//   faudra mettre les bons offsets (sinon on recompute)
//     ou bien dire: on deploy à partir de là (WHERE > timestamp/event/deployment_id)
//   dans un catalog / app qui s'appelerait: admin et où les lectures sont restreintes
// TODO et on aurait une admin en react, avec toutes ces infos

public class KafkaSchemaRegistry {

    public static final String MOB_CLUSTER_IO_FLOW = "mob.cluster-io.flow"; // in, out
    public static final String MOB_CLUSTER_IO_TYPE = "mob.cluster-io.type"; // client, js-engine, service
    public static final String MOB_CLUSTER_IO_JS_ENGINE_CODE = "mob.js-engine.code"; // location of file containting the code
    public static final String MOB_CLUSTER_IO_JS_ENGINE_INVOKE_FUNCTION = "mob.js-engine.invoke-function"; // location of file containing the code

    public static final Map<String, String> DEFAULT_CONFIGURATION = new HashMap<>();
    static {
        // TODO c'est brittle que client soit par defaut:
        //  un oubli d'un app dev, et puis les clients peuvent écrire dans le service
        //  -> faire une faire politique de sécu, déclarer les services requis autrement?
        //    locker l'écriture dans ces topics au dessus des apps
        DEFAULT_CONFIGURATION.put(KafkaSchemaRegistry.MOB_CLUSTER_IO_TYPE, "client");
    }

    static final Map<String, String> schemas = new HashMap<>();
    static final Map<String, Map<String, String>> mobOptions = new HashMap<>();

    public static void registerSchema(String tableName, String jsonSchema) {
        // TODO Write to kafka, with properties, with category parsed, with client_accessible, schema, etc ? "TableRegistry"
        schemas.put(tableName, jsonSchema);
    }

    public static void registerMobOptions(String tableName,  Map<String, String> options) {
        Map<String, String> fromDefault = new HashMap<>(DEFAULT_CONFIGURATION);
        fromDefault.putAll(options);
        mobOptions.put(tableName, fromDefault);
    }

    static public Map<String, String> getInputSchemas() {
        return getSchemas("in");
    }

    static  public Map<String, String> getOutputSchemas() {
        return getSchemas("out");
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
            return mobOptions.entrySet().stream()
                    .filter(it -> null != it.getValue())
                    .filter(it -> "client".equals(it.getValue().get(KafkaSchemaRegistry.MOB_CLUSTER_IO_TYPE))) // TODO faire par defaut client? ou bien demander?
                    .filter(it -> it.getValue().get(KafkaSchemaRegistry.MOB_CLUSTER_IO_FLOW).equals(category))
                    .filter(it -> null != it.getKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, it -> schemas.get(it.getKey())));
        } catch (NullPointerException e) {
            throw new RuntimeException("Your kafka tables probably did not contain sufficient metadata or did not contain the right one", e);
        }
    }

    public static Map<String, Map<String, String>> getJsEngineConfiguration() {
        // strings pour le moment
        return mobOptions.entrySet().stream()
                .filter(it -> "js-engine".equals(it.getValue().get(KafkaSchemaRegistry.MOB_CLUSTER_IO_TYPE)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
