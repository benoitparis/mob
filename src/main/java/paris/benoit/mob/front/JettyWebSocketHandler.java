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
import java.util.concurrent.ArrayBlockingQueue;
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
    private volatile boolean isRunning = true;

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        logger.debug("Close: statusCode=" + statusCode + ", reason=" + reason);
        JettyClusterReceiver.unRegister(this);
        isRunning = false;
    }

    @OnWebSocketError
    public void onError(Throwable t) {
        logger.error("Error: " + t.getMessage());
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        logger.debug("onConnect: session=" + session);
        remote = session.getRemote();
        name = "fa-" + UUID.randomUUID().toString();
        clusterSendersFuture = ClusterRegistry.getClusterSender(name);
        try {
            clusterSenders = clusterSendersFuture.get();
        } catch (InterruptedException e) {
            logger.debug("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.debug("ExecutionException", e);
        }
        JettyClusterReceiver.register(this);

    }

    @OnWebSocketMessage
    public void onMessage(String msg) {

        ToServerMessage cMsg = new ToServerMessage(name, msg);

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
                        specificSender.sendMessage(cMsg);
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

    ArrayBlockingQueue<ToClientMessage> queue = new ArrayBlockingQueue<>(100);
    {
        // FIXME can't wait for virtual threads
        new Thread(() -> {
            while(isRunning) {
                try {
                    ToClientMessage msg = queue.take();
                    // one at a time
                    remote.sendString(msg.toString());
                } catch (InterruptedException | IOException e) {
                    logger.error("error in dequeing to send client", e);
                }
            }
        }).start();
    }
    public void processServerMessage(ToClientMessage message) {
        try {
            queue.put(message);
        } catch (Exception e) {
        }
    }

}