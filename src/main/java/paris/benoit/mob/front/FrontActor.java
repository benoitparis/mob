package paris.benoit.mob.front;

import java.util.concurrent.ThreadLocalRandom;

import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.BasicActor;
import co.paralleluniverse.actors.ExitMessage;
import co.paralleluniverse.actors.LifecycleMessage;
import co.paralleluniverse.comsat.webactors.WebActor;
import co.paralleluniverse.comsat.webactors.WebDataMessage;
import co.paralleluniverse.comsat.webactors.WebSocketOpened;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.SendPort;
import paris.benoit.mob.loopback.ActorEntrySource;
import paris.benoit.mob.message.ClientMessage;
import paris.benoit.mob.message.ClusterMessage;
import paris.benoit.mob.message.Message;

@SuppressWarnings("serial")
@WebActor(webSocketUrlPatterns = {"/service/ws"})
public class FrontActor extends BasicActor<Object, Void> {
    // TODO use properly
//    static Logger LOG = LoggerFactory.getLogger(FrontActor.class);

    private boolean initialized;
    private SendPort<WebDataMessage> clientWSPort;
    
    public FrontActor() {
        super("fa-" + ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
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
                System.out.println("Registering WS port");
            }
            // -------- WebSocket message received -------- 
            else if (message instanceof WebDataMessage) {
                WebDataMessage msg = (WebDataMessage) message;
                System.out.println("Got a WS message: " + msg);
                
                ClientMessage cMsg = new ClientMessage(msg.getStringBody());
                
                switch (cMsg.intent) {
                case INFO: System.out.println(cMsg.payload);
                    break;
                case QUERY: System.out.println(cMsg.payload); 
                    break;
                case CALL: System.out.println(cMsg.payload); // TODO
                    break;
                case SUBSCRIBE: System.out.println(cMsg.payload); // TODO
                    break;
                }
                
                
                ActorEntrySource.send(new ClusterMessage(getName(), new Message("ClusterMessage!" + msg.getStringBody())));
            }
            // Message from LoopBackSink
            else if (message instanceof ClusterMessage) {
                ClusterMessage msg = (ClusterMessage) message;
                if (null != clientWSPort) {
                    clientWSPort.send(new WebDataMessage(self(), msg.getPayload().getContent()));
                } else {
                    System.out.println("Received a message from the cluster without having a WS Port to send it back to");
                }
            }
            else if (null == message) {
                System.out.println("null message");
            } else {
                System.out.println("Unknown message type :" + message);
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
            System.out.println("Actor " + em.getActor() + " has died.");
            // do things?
            //boolean res = listeners.remove(em.getActor());
            // System.out.println((res ? "Successfuly" : "Unsuccessfuly") + " removed listener for actor " + em.getActor());
        }
        return super.handleLifecycleMessage(m);
    }
}