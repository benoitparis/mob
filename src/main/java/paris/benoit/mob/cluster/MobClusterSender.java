package paris.benoit.mob.cluster;

import java.io.IOException;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;

// TODO fusionner avec ActorSource, channel, receivePort, jrds largment partagé, et la construction de row à deux endroits
//   et en plus l'acteur pourra faure un Sources.getChannels, ce qui sémantiquement est plus propre, et clyclomatiquement ne lie plus .cluster à actors
//   quand multi-sources
public class MobClusterSender {
    
    private JsonRowDeserializationSchema jrds;
    private Channel<Row> channel;
    ThreadReceivePort<Row> receiveport;
    
    public MobClusterSender(JsonRowDeserializationSchema jrds) {
        super();
        this.jrds = jrds;
        this.channel = Channels.newChannel(100_000, OverflowPolicy.BACKOFF, false, false);
        this.receiveport = new ThreadReceivePort<Row>(channel);
    }
    
    public void send(String identity, String payload) throws SuspendExecution, InterruptedException {
        
        try {

            Row root = new Row(3);
            // 0 is loopbackIndex, by convention; to be set by the function
            root.setField(1, identity);
            root.setField(2, jrds.deserialize(payload.getBytes()));
            channel.send(root);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public ThreadReceivePort<Row> getReceiveport() {
        return receiveport;
    }
}