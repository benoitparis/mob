package paris.benoit.mob.loopback;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.types.Row;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.message.ClusterMessage;

// TODO rename après générique, le json deviendra T générique
@SuppressWarnings("serial")
public class JsonActorEntrySource extends RichParallelSourceFunction<Row> {
    
    // static + non-static not good
    private static Channel<ClusterMessage> channelToSource = Channels.newChannel(1000000, OverflowPolicy.BACKOFF, false, false);
    private static ThreadReceivePort<ClusterMessage> receivePort = new ThreadReceivePort<ClusterMessage>(channelToSource);
    
    JsonRowDeserializationSchema jrds = null;
    public JsonActorEntrySource(TypeInformation<Row> jsonTypeInfo) {
        this.jrds = new JsonRowDeserializationSchema(jsonTypeInfo);
    }

    private volatile boolean isRunning = true;
    private Integer backChannel = null;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Assumption: The number of Sources must be greater or equal to the number of Sinks on the same JVM.
        backChannel = ActorLoopBackSink.registerQueue.take();
        System.out.println("Opening JsonActorEntrySource, backChannel #" + backChannel);
    }
    
    public void run(SourceContext<Row> sc) throws Exception {
        
        while (isRunning && !receivePort.isClosed()) {
            Row root = new Row(1);
            final String payloadContent = receivePort.receive().getPayload().getContent();
            Row payload = jrds.deserialize(payloadContent.getBytes());
            root.setField(0, payload);
            sc.collect(root);
        }
    }
    
    public void cancel() {
        isRunning = false;
    }
    
    public static void send(ClusterMessage message) throws SuspendExecution, InterruptedException {
        System.out.println("putting msg in channel: " + message);
        channelToSource.send(message);
    }

}
