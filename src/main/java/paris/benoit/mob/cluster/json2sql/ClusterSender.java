package paris.benoit.mob.cluster.json2sql;

import java.io.IOException;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.Channel;

public class ClusterSender {
    
    private Channel<Row> channel;
    private JsonRowDeserializationSchema jrds;
    
    public ClusterSender(Channel<Row> channel, JsonRowDeserializationSchema jrds) {
        super();
        this.channel = channel;
        this.jrds = jrds;
    }
    
    public void send(String identity, String payload) throws SuspendExecution, InterruptedException {
        
        try {
            Row payloadRow = jrds.deserialize(payload.getBytes());
            Row root = new Row(3);
            // 0 is loopbackIndex, by convention; to be set by the function
            root.setField(1, identity);
            root.setField(2, payloadRow);
            channel.send(root);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}