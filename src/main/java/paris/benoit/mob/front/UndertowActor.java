package paris.benoit.mob.front;

import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.actors.ExitMessage;
import co.paralleluniverse.actors.LifecycleMessage;
import co.paralleluniverse.comsat.webactors.WebActor;
import co.paralleluniverse.comsat.webactors.WebDataMessage;
import co.paralleluniverse.comsat.webactors.WebSocketOpened;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.SendPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.loopback.ClusterRegistry;
import paris.benoit.mob.cluster.loopback.ClusterSender;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.message.ToServerMessage;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("serial")
@WebActor(webSocketUrlPatterns = {"/service/ws"})
public class UndertowActor extends BasicActor<Object, Void> {
    private static final Logger logger = LoggerFactory.getLogger(UndertowActor.class);

    private SendPort<WebDataMessage> clientWSPort;
    private CompletableFuture<Map<String, ClusterSender>> clusterSendersFuture;
    
    public UndertowActor() throws InterruptedException {
        super("fa-" + UUID.randomUUID().toString());
        clusterSendersFuture = ClusterRegistry.getClusterSender(getName());
        logger.debug("new UndertowActor: " + getName());
    }

    @Override
    protected Void doRun() throws InterruptedException, SuspendExecution {

        register();
        Map<String, ClusterSender> clusterSenders;
        try {
            clusterSenders = clusterSendersFuture.get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        for (;;) {
            Object message = receive();
            if (message instanceof WebSocketOpened) {
                WebSocketOpened msg = (WebSocketOpened) message;
                ActorRef<WebDataMessage> from = msg.getFrom();
                watch(from); // will call handleLifecycleMessage with ExitMessage when the session ends
                clientWSPort = from;
                logger.debug("WebSocketOpened: " + msg.toString());
            }
            else if (message instanceof WebDataMessage) {
                WebDataMessage msg = (WebDataMessage) message;
                ToServerMessage cMsg = new ToServerMessage(msg.getStringBody());
                
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
                                specificSender.sendMessage(getName(), cMsg.payload.toString());
                            } catch (Exception e) {
                                logger.error("error in sending back message", e);
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
            else if (message instanceof ToClientMessage) {
                ToClientMessage msg = (ToClientMessage) message;
                if (null != clientWSPort) {
                    clientWSPort.send(new WebDataMessage(self(), msg.toString()));
                } else {
                    logger.warn("Received a message from the cluster without having a WS Port to sendMessage it back to");
                }
            }
            else if (null == message) {
                logger.warn("null message");
            } else {
                logger.warn("Unknown message type :" + message);
            }
        }
    }

    @Override
    protected Object handleLifecycleMessage(LifecycleMessage m) {
        if (m instanceof ExitMessage) {
            // while listeners might contain an SSE actor wrapped with Channels.map, 
            // the wrapped SendPort maintains the original actors hashCode and equals behavior
            ExitMessage em = (ExitMessage) m;
            logger.debug("Actor " + em.getActor() + " has died.");
        }
        return super.handleLifecycleMessage(m);
    }
}