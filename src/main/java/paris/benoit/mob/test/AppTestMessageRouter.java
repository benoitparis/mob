package paris.benoit.mob.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.MessageRouter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class AppTestMessageRouter implements MessageRouter {
    private static final Logger logger = LoggerFactory.getLogger(AppTestMessageRouter.class);

    static List<ClientSimulator> simulators = new CopyOnWriteArrayList<>();

    public static void registerClientSimulator(ClientSimulator clientSimulator) {
        logger.debug("Registering ClientSimulator: " + clientSimulator.getName());
        simulators.add(clientSimulator);
    }

    @Override
    public void routeMessage(Integer loopbackIndex, String identity, ToClientMessage message) {

        simulators.stream().forEach(it -> it.offerMessage(loopbackIndex, identity, message));

    }
}
