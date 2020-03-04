package paris.benoit.mob.front;

import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

import java.util.concurrent.ConcurrentHashMap;

public class JettyClusterReceiver implements ClusterReceiver {

    private static ConcurrentHashMap<String, JettyWebSocketHandler> clients = new ConcurrentHashMap<>();

    public static void register(JettyWebSocketHandler handler) {
        clients.put(handler.name, handler);
    }
    public static void unRegister(JettyWebSocketHandler handler) {
        clients.remove(handler.name);
    }

    @Override
    public void receiveMessage(Integer loopbackIndex, String identity, ToClientMessage message) {
        clients.get(identity).processServerMessage(message);

    }

}
