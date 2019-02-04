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
import paris.benoit.mob.cluster.RegistryWeaver;
import paris.benoit.mob.cluster.json2sql.ClusterSender;
import paris.benoit.mob.message.ClientMessage;

@SuppressWarnings("serial")
@WebActor(webSocketUrlPatterns = {"/service/ws"})
public class FrontActor extends BasicActor<Object, Void> {

    private boolean initialized;
    private SendPort<WebDataMessage> clientWSPort;
    private ClusterSender clusterSender;
    
    public FrontActor() throws InterruptedException {
        super("fa-" + ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        clusterSender = RegistryWeaver.getClusterSender(getName());
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
                
                // clunky, badly named, et en plus on seri/déséri deux fois
                // faudrait ptet deux niveau de jsonschema?
                //   l'un technique, qui prend une ref de l'autre
                //   et on map ici, on fait du tech ici, avant d'envoyer à l'inputTable
                //   ref sur schema: https://stackoverflow.com/questions/18376215/jsonschema-split-one-big-schema-file-into-multiple-logical-smaller-files
                //   avec pitetre un resolver que tu fais toi-même et que tu inline la chose
                //   au lieu de $ref, le nom $param serait plus approprié?
                //     et tu peux même typer sur plusieurs flux de tables ici?
                //       en tout cas pas contraint par les schemas flink; tu en fera un par table
                // ici on s'envoie a soi-même, alors que c'est business.
                //   faudra mettre le renvoi à soi-même dans le SQL
                
                clusterSender.send(getName(), cMsg.payload.toString());
            }
            // Message from LoopBackSink
            // String pas ouf niveau typage? faudrait ptet un wrapper? Json ça fait un coup de serde en plus.. ou bien un Row?
            //   on fait la serde où?? ptet ici non?
            else if (message instanceof String) {
                String msg = (String) message;
                if (null != clientWSPort) {
                    clientWSPort.send(new WebDataMessage(self(), msg));
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