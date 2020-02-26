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
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("serial")
@WebActor(webSocketUrlPatterns = {"/service/ws"})
public class FrontActor extends BasicActor<Object, Void> {
    private static final Logger logger = LoggerFactory.getLogger(FrontActor.class);

    private boolean initialized;
    private SendPort<WebDataMessage> clientWSPort;
    private Map<String, ClusterSender> clusterSenders;
    
    public FrontActor() throws InterruptedException {
        // guids? 
        // index-atomicIncrements? index pour debug niveau client? atomicIncrements dans les sources, au getChannels? 
        //   des NumberedChannels? oui, et on fait le send+setfield+deseri là?
        // du coup on donnerait pas que channel?
        super("fa-" + ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        clusterSenders = ClusterRegistry.getClusterSender(getName());
    }

    @Override
    protected Void doRun() throws InterruptedException, SuspendExecution {
        if (!initialized) { // necessary for hot code swapping, as this method might be called again after swap
            this.initialized = true;
        }
        register();
        
        for (;;) {
            Object message = receive();
            if (message instanceof WebSocketOpened) {
                WebSocketOpened msg = (WebSocketOpened) message;
                ActorRef<WebDataMessage> from = msg.getFrom();
                watch(from); // will call handleLifecycleMessage with ExitMessage when the session ends
                clientWSPort = from;
                logger.debug("Registering WS port");
            }
            else if (message instanceof WebDataMessage) {
                WebDataMessage msg = (WebDataMessage) message;
                //logger.debug("Got a WS message: " + msg);
                
                ToServerMessage cMsg = new ToServerMessage(msg.getStringBody());
                
                // !on seri/déséri deux fois
                //   y revenir plus tard avec deux niveaux de jsonschema et un resolver perso qui inline
                //   ref sur schema: https://stackoverflow.com/questions/18376215/jsonschema-split-one-big-schema-file-into-multiple-logical-smaller-files

                // TODO DRY avec ClientSimulator
                switch (cMsg.intent) {
                case WRITE: 
                    {
                        ClusterSender specificSender = clusterSenders.get(cMsg.table);
                        if (null == specificSender) {
                            logger.warn("A ClusterSender (table destination) was not found: " + cMsg.table);
                        } else {
                            specificSender.send(getName(), cMsg.payload.toString());
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
                    logger.warn("Received a message from the cluster without having a WS Port to send it back to");
                }
            }
            else if (null == message) {
                logger.warn("null message");
            } else {
                logger.warn("Unknown message type :" + message);
            }
            
            checkCodeSwap();
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