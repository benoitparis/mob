package paris.benoit.mob.cluster.external.js;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.loopback.distributed.KafkaSchemaRegistry;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// TODO REM si la latence est trop haute, passer en datastream avec:
//   catalog.createTable(
//           sourceConf.getObjectPath(),
//           ConnectorCatalogTable.source(source, false),
//           false
//   );

public class ExternalJsEngine {
    private static final Logger logger = LoggerFactory.getLogger(ExternalJsEngine.class);

    public static void scanAndCreateJsEngine() throws IOException, ScriptException {

        Map<String, Properties> jsConf = KafkaSchemaRegistry.getJsEngineConfiguration();
        logger.info("Creating js engine with schemas: " + jsConf);
        if (null != jsConf && jsConf.size() > 0) {
            if (2 < jsConf.size()) {
                throw new RuntimeException("Only one js engine can be created at a time");
            }
            // TODO avoir les deux dasn une même fichier?
            // TODO enlever les properties

            String fileCodeLocation = jsConf.get("out").getProperty(MobTableConfiguration.MOB_CLUSTER_IO_JS_ENGINE_CODE);
            // TODO un-hard-code it (pong)
            String sourceCode = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/pong/" + fileCodeLocation)));

            String invokeFunction = jsConf.get("out").getProperty(MobTableConfiguration.MOB_CLUSTER_IO_JS_ENGINE_INVOKE_FUNCTION);

            // TODO chopper le name autrement
            String tableNameInEngine = jsConf.get("out").getProperty(MobTableConfiguration.MOB_TABLE_NAME);
            String tableNameOutEngine = jsConf.get("in").getProperty(MobTableConfiguration.MOB_TABLE_NAME);

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            // TODO magic value
            props.put("group.id", jsConf.get("out").getProperty(MobTableConfiguration.MOB_CLUSTER_IO_TYPE));
            props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
            props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

            ScriptEngine graaljsEngine = new ScriptEngineManager().getEngineByName("graal.js");
            graaljsEngine.eval(sourceCode);
            Invocable inv = (Invocable) graaljsEngine;

            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            consumer.subscribe(Collections.singleton(tableNameInEngine));
            // TODO another class
            // TODO graceful shutdown
            new Thread(() -> {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    records.records(tableNameInEngine).forEach((it) -> {
                        try {
                            // TODO get standard output / err to a Kafka topic
                            Map out = (Map) inv.invokeFunction(invokeFunction, it.value());
                            @SuppressWarnings("unchecked") HashMap copy = new HashMap(out); // defensive copying
                            String result = convertMapToJsonString(copy);

                            ProducerRecord<String, String> msg = new ProducerRecord<>(tableNameOutEngine, result);
                            producer.send(msg);

                        } catch (ScriptException | NoSuchMethodException | JsonProcessingException e) {
                            logger.error("Error executing scripting engine", e);
                        }
                    });
                }
            }).start();

        }

    }

    private static final ObjectMapper mapper = new ObjectMapper();

    private static String convertMapToJsonString(Map map) throws JsonProcessingException {
        return mapper.writeValueAsString(map);
    }

}
