package paris.benoit.mob.front;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

// TODO changer cette combinaison de calls static et non-static, c'est moche
public class JettyClusterReceiver implements ClusterReceiver {
    private static final Logger logger = LoggerFactory.getLogger(JettyClusterReceiver.class);
    public static final String MOBCATALOG_SERVICES_CLIENT_SESSION = "mobcatalog.services.client_session";

    private static ConcurrentHashMap<String, JettyClusterMessageProcessor> clients = new ConcurrentHashMap<>();
    private static final KafkaProducer<String, String> producer;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public static void register(String name, JettyClusterMessageProcessor handler) {
        clients.put(name, handler);
        producer.send(new ProducerRecord<>(MOBCATALOG_SERVICES_CLIENT_SESSION, "{\"client_id\":\"" + name + "\", \"active\":true}"));
    }

    public static void unRegister(String name) {
        // leak here: TODO manage better front handler lifecycle
        //clients.remove(name);
        clients.put(name, (it) -> {});
        producer.send(new ProducerRecord<>(MOBCATALOG_SERVICES_CLIENT_SESSION, "{\"client_id\":\"" + name + "\", \"active\":false}"));
    }

    @Override
    public void receiveMessage(ToClientMessage message) {
        System.out.println(message);
        JettyClusterMessageProcessor client = clients.get(message.client_id);

        if (null != client) {
            client.processServerMessage(message);
        } else {
            logger.warn("Unable to find client: " + message.client_id);
        }
    }



}
