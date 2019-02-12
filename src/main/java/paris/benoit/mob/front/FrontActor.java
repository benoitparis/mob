package paris.benoit.mob.front;

import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.actors.ExitMessage;
import co.paralleluniverse.actors.LifecycleMessage;
import co.paralleluniverse.comsat.webactors.WebActor;
import co.paralleluniverse.comsat.webactors.WebDataMessage;
import co.paralleluniverse.comsat.webactors.WebSocketOpened;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.SendPort;
import paris.benoit.mob.cluster.ClusterRegistry;
import paris.benoit.mob.cluster.json2sql.ClusterSender;
import paris.benoit.mob.message.ClientMessage;

@SuppressWarnings("serial")
@WebActor(webSocketUrlPatterns = {"/service/ws"})
public class FrontActor extends BasicActor<Object, Void> {
    private static final Logger logger = LoggerFactory.getLogger(FrontActor.class);

    private boolean initialized;
    private SendPort<WebDataMessage> clientWSPort;
    private ClusterSender clusterSender;
    
    public FrontActor() throws InterruptedException {
        super("fa-" + ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        clusterSender = ClusterRegistry.getClusterSender(getName());
    }

    @Override
    protected Void doRun() throws InterruptedException, SuspendExecution {
        if (!initialized) { // necessary for hot code swapping, as this method might be called again after swap
            this.initialized = true;
        }
        register();
        
        for (;;) {
            Object message = receive();
            // -------- WebSocket opened --------
            if (message instanceof WebSocketOpened) {
                WebSocketOpened msg = (WebSocketOpened) message;
                ActorRef<WebDataMessage> from = msg.getFrom();
                watch(from); // will call handleLifecycleMessage with ExitMessage when the session ends
                clientWSPort = from;
                logger.debug("Registering WS port");
            }
            // -------- WebSocket message received -------- 
            else if (message instanceof WebDataMessage) {
                WebDataMessage msg = (WebDataMessage) message;
                logger.debug("Got a WS message: " + msg);
                
                ClientMessage cMsg = new ClientMessage(msg.getStringBody());
                
                // !on seri/déséri deux fois
                //   y revenir plus tard avec deux niveaux de jsonschema et un resolver perso qui inline
                //   ref sur schema: https://stackoverflow.com/questions/18376215/jsonschema-split-one-big-schema-file-into-multiple-logical-smaller-files

                switch (cMsg.intent) {
                case WRITE: clusterSender.send(getName(), cMsg.payload.toString());
                    break;
                case QUERY: logger.debug(cMsg.payload.toString()); 
                    break;
                case SUBSCRIBE: logger.debug(cMsg.payload.toString());
                    break;
                }
                

            }
            // Message from LoopBackSink
            // String pas ouf niveau typage? faudrait ptet un wrapper? Json ça fait un coup de serde en plus.. ou bien un Row?
            //   on fait la serde où?? ptet ici non?
            else if (message instanceof String) {
                String msg = (String) message;
                if (null != clientWSPort) {
                    clientWSPort.send(new WebDataMessage(self(), msg));
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
            // do things?
            //boolean res = listeners.remove(em.getActor());
            // logger.info((res ? "Successfuly" : "Unsuccessfuly") + " removed listener for actor " + em.getActor());
        }
        return super.handleLifecycleMessage(m);
    }
}