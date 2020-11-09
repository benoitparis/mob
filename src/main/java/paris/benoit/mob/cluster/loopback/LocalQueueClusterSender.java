package paris.benoit.mob.cluster.loopback;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterSender;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Wrapper class, used for Sources to poll messages sent from the front.
 */
public class LocalQueueClusterSender implements ClusterSender {

    private final ArrayBlockingQueue<ToServerMessage> queue = new ArrayBlockingQueue<ToServerMessage>(100_000);

    @Override
    public void sendMessage(ToServerMessage message) throws Exception {
        queue.put(message);
    }

    @Override
    public ToServerMessage receive() throws Exception {
        return queue.take();
    }


    public static void registerClusterSender(String fullName, ClusterSender sender, Integer loopbackIndex) {

    }

    public static void waitRegistrationsReady() throws InterruptedException {

    }


    private static void doClusterSendersMatching(int parallelism, MobClusterConfiguration configuration) {

    }

}