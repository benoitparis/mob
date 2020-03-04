package paris.benoit.mob.cluster.loopback;

import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.ActorRegistry;
import co.paralleluniverse.fibers.SuspendExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

public class UndertowActorClusterReceiver implements ClusterReceiver {
    private static final Logger logger = LoggerFactory.getLogger(ActorSource.class);

    @Override
    public void receiveMessage(Integer loopbackIndex, String identity, ToClientMessage message) {
        ActorRef<ToClientMessage> actor = null;
        try {
            actor = (ActorRef<ToClientMessage>) ActorRegistry.tryGetActor(identity);
        } catch (SuspendExecution suspendExecution) {
            logger.error("Exception while getting the actor ", suspendExecution);
        }
        if (null != actor) {
            // call to sendMessage: not blocking or dropping the message, as his mailbox is unbounded
            try {
                actor.send(message);
            } catch (SuspendExecution suspendExecution) {
                suspendExecution.printStackTrace();
            }
        } else {
            logger.error("Actor named " + identity + " was not found with loopbackIndex " + loopbackIndex);
        }
    }
}
