package paris.benoit.mob.cluster;

import java.io.IOException;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;

public class MobClusterSender {
    
    private JsonRowDeserializationSchema jrds;
    private Channel<Row> channel;
    ThreadReceivePort<Row> receiveport;
    
    public MobClusterSender(JsonRowDeserializationSchema jrds) {
        super();
        this.jrds = jrds;
        this.channel = Channels.newChannel(1000000, OverflowPolicy.BACKOFF, false, false);
        this.receiveport = new ThreadReceivePort<Row>(channel);
    }
    
    public void send(String identity, String payload) throws SuspendExecution, InterruptedException {
        
        try {
            Row payloadRow = jrds.deserialize(payload.getBytes());
            //
            Row root = new Row(3);
            // 0 is loopbackIndex, by convention; to be set by the function
            root.setField(1, identity);
            root.setField(2, payloadRow);
            channel.send(root);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public ThreadReceivePort<Row> getReceiveport() {
        return receiveport;
    }
}