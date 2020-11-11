package paris.benoit.mob.cluster.loopback.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterSender;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Wrapper class, used for Sources to poll messages sent from the front.
 */
public class LocalQueueClusterSender implements ClusterSender {
    private static final Logger logger = LoggerFactory.getLogger(LocalQueueClusterSender.class);

    // NON STATIC
    private final ArrayBlockingQueue<ToServerMessage> queue = new ArrayBlockingQueue<ToServerMessage>(100_000);

    @Override
    public void sendMessage(ToServerMessage message) throws Exception {
        queue.put(message);
    }

    @Override
    public ToServerMessage receive() throws Exception {
        return queue.take();
    }

}