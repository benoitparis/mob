package paris.benoit.mob.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class AppTestMessageRouter implements ClusterReceiver {
    private static final Logger logger = LoggerFactory.getLogger(AppTestMessageRouter.class);

    private static final List<ClientSimulator> simulators = new CopyOnWriteArrayList<>();

    public static void registerClientSimulator(ClientSimulator clientSimulator) {
        logger.debug("Registering ClientSimulator: " + clientSimulator.getName());
        simulators.add(clientSimulator);
    }

    @Override
    public void receiveMessage(Integer loopbackIndex, String identity, ToClientMessage message) {

        simulators.forEach(it -> it.offerMessage(loopbackIndex, identity, message));

    }
}
