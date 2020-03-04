package paris.benoit.mob.front;


import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.loopback.ClusterRegistry;
import paris.benoit.mob.cluster.loopback.ClusterSender;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.message.ToServerMessage;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

// TODO refactor ça avec le UndertowActor et ClientSimulator
@WebSocket
public class JettyWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(JettyWebSocketHandler.class);
    public String name;

    private CompletableFuture<Map<String, ClusterSender>> clusterSendersFuture;
    private Map<String, ClusterSender> clusterSenders;
    private RemoteEndpoint remote;

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        System.out.println("Close: statusCode=" + statusCode + ", reason=" + reason);
        JettyClusterReceiver.unRegister(this);
    }

    @OnWebSocketError
    public void onError(Throwable t) {
        System.out.println("Error: " + t.getMessage());
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        remote = session.getRemote();
        name = "fa-" + UUID.randomUUID().toString();
        clusterSendersFuture = ClusterRegistry.getClusterSender(name);
        try {
            clusterSenders = clusterSendersFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        JettyClusterReceiver.register(this);

        System.out.println("Connect: " + session.getRemoteAddress().getAddress());
        try {
            session.getRemote().sendString("Hello Webbrowser");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {
        System.out.println("Message  uuuuiiiiii: " + msg);

        ToServerMessage cMsg = new ToServerMessage(msg);

        // TODO !on seri/déséri deux fois
        //   utiliser avro, et s'envoyer des subsets

        // TODO DRY avec ClientSimulator
        switch (cMsg.intent) {
            case WRITE:
            {
                ClusterSender specificSender = clusterSenders.get(cMsg.table);
                if (null == specificSender) {
                    logger.warn("A ClusterSender (table destination) was not found: " + cMsg.table);
                } else {
                    try {
                        specificSender.sendMessage(name, cMsg.payload.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            break;
            case QUERY: logger.debug(cMsg.payload.toString());
                break;
            case SUBSCRIBE: logger.debug(cMsg.payload.toString());
                break;
        }
    }

    public void processServerMessage(ToClientMessage message) {
        try {
            remote.sendString(message.toString());
        } catch (Exception e) {
            logger.error("error in sending back message", e);
        }
    }
}