package paris.benoit.mob.front;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

import java.util.concurrent.ConcurrentHashMap;

public class JettyClusterReceiver implements ClusterReceiver {
    private static final Logger logger = LoggerFactory.getLogger(JettyClusterReceiver.class);

    private static ConcurrentHashMap<String, JettyClusterMessageProcessor> clients = new ConcurrentHashMap<>();

    public static void register(String name, JettyClusterMessageProcessor handler) {
        clients.put(name, handler);
    }
    public static void unRegister(String name) {
        // leak here: TODO manage better front handler lifecycle
        //clients.remove(name);
        clients.put(name, (it) -> {});
    }

    @Override
    public void receiveMessage(ToClientMessage message) {
        JettyClusterMessageProcessor client = clients.get(message.client_id);

        if (null != client) {
            client.processServerMessage(message);
        } else {
            logger.warn("Unable to find client: " + message.client_id);
        }
    }

}
