package paris.benoit.mob.front;


import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.loopback.ClusterRegistry;
import paris.benoit.mob.cluster.loopback.LocalQueueClusterSender;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterSender;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

// TODO refactor ça avec le ClientSimulator
@WebSocket
public class JettyWebSocketHandler implements JettyClusterMessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(JettyWebSocketHandler.class);
    public String name;

    private CompletableFuture<Map<String, ClusterSender>> clusterSendersFuture;
    private Map<String, ClusterSender> clusterSenders;
    private RemoteEndpoint remote;
    private volatile boolean isRunning = true;

    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        logger.debug("Close: statusCode=" + statusCode + ", reason=" + reason);
        JettyClusterReceiver.unRegister(this.name);
        isRunning = false;
    }

    @OnWebSocketError
    public void onError(Throwable t) {
        JettyClusterReceiver.unRegister(this.name);
        logger.error("Error: " + t.getMessage());
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        logger.debug("onConnect: session=" + session);
        remote = session.getRemote();
        name = "fa-" + UUID.randomUUID().toString();
        clusterSendersFuture = ClusterRegistry.getClusterSenders(name);
        try {
            clusterSenders = clusterSendersFuture.get();
        } catch (InterruptedException e) {
            logger.debug("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.debug("ExecutionException", e);
        }
        JettyClusterReceiver.register(this.name, this);

    }

    @OnWebSocketMessage
    public void onMessage(String msg) {

        ToServerMessage cMsg = new ToServerMessage(name, msg);

        // TODO !on seri/déséri deux fois
        //   utiliser avro, et s'envoyer des subsets

        // TODO DRY avec ClientSimulator
        ClusterSender specificSender = clusterSenders.get(cMsg.table);
        if (null == specificSender) {
            logger.warn("A ClusterSender (table destination) was not found: " + cMsg.table);
        } else {
            try {
                specificSender.sendMessage(cMsg);
            } catch (Exception e) {
                logger.error("error in sending message", e);
            }
        }
    }

    ArrayBlockingQueue<ToClientMessage> queue = new ArrayBlockingQueue<>(100);
    {
        // FIXME can't wait for virtual threads
        //   actually orthogonal. use futures?
        new Thread(() -> {
            while(isRunning) {
                try {
                    ToClientMessage msg = queue.take();
                    // one at a time
                    remote.sendString(msg.toJson());
                } catch (InterruptedException | IOException e) {
                    logger.error("error in dequeing to send client", e);
                }
            }
        }).start();
    }
    @Override
    public void processServerMessage(ToClientMessage message) {
        try {
            queue.put(message);
        } catch (Exception e) {
        }
    }

}