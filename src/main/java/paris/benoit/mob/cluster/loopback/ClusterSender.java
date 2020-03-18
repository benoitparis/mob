package paris.benoit.mob.cluster.loopback;

import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.message.ToServerMessage;

public class ClusterSender {

    private final Channel<ToServerMessage> channel;
    private final ThreadReceivePort<ToServerMessage> receivePort;
    
    public ClusterSender() {
        super();
        this.channel = Channels.newChannel(100_000, OverflowPolicy.BACKOFF, false, false);
        this.receivePort = new ThreadReceivePort<>(channel);
    }
    
    public void sendMessage(ToServerMessage message) throws Exception {
        channel.send(message);
    }
    
    public ThreadReceivePort<ToServerMessage> getReceivePort() {
        return receivePort;
    }

    public boolean isClosed() {
        return receivePort.isClosed();
    }

    public ToServerMessage receive() throws Exception {
        return receivePort.receive();
    }

}