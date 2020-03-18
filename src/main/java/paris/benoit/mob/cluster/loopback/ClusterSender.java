package paris.benoit.mob.cluster.loopback;

import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.message.ToServerMessage;

// TODO jrds largment partagé, et la construction de row à deux endroits
public class ClusterSender {

    private final Channel<ToServerMessage> channel;
    private final ThreadReceivePort<ToServerMessage> receiveport;
    
    public ClusterSender() {
        super();
        this.channel = Channels.newChannel(100_000, OverflowPolicy.BACKOFF, false, false);
        this.receiveport = new ThreadReceivePort<>(channel);
    }
    
    public void sendMessage(ToServerMessage message) throws Exception {
        channel.send(message);
    }
    
    public ThreadReceivePort<ToServerMessage> getReceiveport() {
        return receiveport;
    }

    public boolean isClosed() {
        return receiveport.isClosed();
    }

    public ToServerMessage receive() throws Exception {
        return receiveport.receive();
    }
}