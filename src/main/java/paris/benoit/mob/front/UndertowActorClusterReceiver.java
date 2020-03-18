package paris.benoit.mob.front;

import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.ActorRegistry;
import co.paralleluniverse.fibers.SuspendExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

public class UndertowActorClusterReceiver implements ClusterReceiver {
    private static final Logger logger = LoggerFactory.getLogger(UndertowActorClusterReceiver.class);

    @Override
    public void receiveMessage(ToClientMessage message) {
        ActorRef<ToClientMessage> actor = null;
        try {
            actor = (ActorRef<ToClientMessage>) ActorRegistry.tryGetActor(message.to);
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
            logger.error("Actor named " + message.to + " was not found");
        }
    }
}
