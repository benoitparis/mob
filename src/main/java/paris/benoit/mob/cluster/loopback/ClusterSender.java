package paris.benoit.mob.cluster.loopback;

import paris.benoit.mob.message.ToServerMessage;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Wrapper class, used for Sources to poll messages sent from the front.
 */
public class ClusterSender {

    private final ArrayBlockingQueue<ToServerMessage> queue = new ArrayBlockingQueue<ToServerMessage>(100_000);

    public void sendMessage(ToServerMessage message) throws Exception {
        queue.put(message);
    }

    public ToServerMessage receive() throws Exception {
        return queue.take();
    }

}