package paris.benoit.mob.front;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

import java.util.concurrent.ConcurrentHashMap;

public class JettyClusterReceiver implements ClusterReceiver {
    private static final Logger logger = LoggerFactory.getLogger(JettyClusterReceiver.class);

    private static ConcurrentHashMap<String, JettyWebSocketHandler> clients = new ConcurrentHashMap<>();

    public static void register(JettyWebSocketHandler handler) {
        clients.put(handler.name, handler);
    }
    public static void unRegister(JettyWebSocketHandler handler) {
        clients.remove(handler.name);
    }

    @Override
    public void receiveMessage(ToClientMessage message) {
        JettyWebSocketHandler client = clients.get(message.to);

        if (null != client) {
            client.processServerMessage(message);
        } else {
            logger.warn("Unable to find client: " + message.to);
        }
    }

}
