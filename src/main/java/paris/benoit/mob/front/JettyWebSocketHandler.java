package paris.benoit.mob.front;


import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.loopback.GlobalClusterSenderRegistry;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterSender;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;

// TODO refactor ça avec le ClientSimulator
@WebSocket
public class JettyWebSocketHandler implements JettyClusterMessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(JettyWebSocketHandler.class);
    public String name;

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
        if (t instanceof TimeoutException) {
            logger.info("Idle client disconnected: " + t.getMessage());
        } else {
            logger.error("Error: " + t.getMessage());
        }

    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        logger.debug("onConnect: session=" + session);
        remote = session.getRemote();
        name = "fa-" + UUID.randomUUID().toString();
        clusterSenders = GlobalClusterSenderRegistry.getClusterSenders(name);
        System.out.println(clusterSenders);
        JettyClusterReceiver.register(this.name, this);
    }

    @OnWebSocketMessage
    public void onMessage(String msg) {

        ToServerMessage cMsg = new ToServerMessage(name, msg);

        // TODO !on seri/déséri deux fois
        //   utiliser avro/protobuf/thrift, s'envoyer des subsets

        // TODO DRY avec ClientSimulator
        ClusterSender specificSender = clusterSenders.get(cMsg.table);
        if (null == specificSender) {
            logger.warn("A ClusterSender (table destination) was not found: " + cMsg.table);
        } else {
            try {
                specificSender.sendMessage(cMsg);
                System.out.println("sent: " + cMsg);
            } catch (Exception e) {
                logger.error("error in sending message", e);
            }
        }
    }

    ArrayBlockingQueue<ToClientMessage> queue = new ArrayBlockingQueue<>(100);
    {
        // FIXME can't wait for virtual threads
        //   actually orthogonal. use futures?
        //   can't wait for scopes? for structured concurrency?
        new Thread(() -> {
            while(isRunning) {
                try {
                    ToClientMessage msg = queue.take();
                    logger.debug("Sent Client: " + msg);
                    // one at a time, could compress?
                    remote.sendString(msg.toJson());
                } catch (InterruptedException | IOException e) {
                    logger.error("error in dequeuing to send client", e);
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