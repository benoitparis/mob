package paris.benoit.mob.loopback;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.json2sql.JsonTableSource;

// TODO rename après générique, le json deviendra T générique
@SuppressWarnings("serial")
public class ActorSource extends RichParallelSourceFunction<Row> {
    
    // ultra sale: on fait une élection plus tot pour remettre ça en static!
    private static Channel<Row> channelToSource = Channels.newChannel(1000000, OverflowPolicy.BACKOFF, false, false);
    private static ThreadReceivePort<Row> receivePort = new ThreadReceivePort<Row>(channelToSource);

    private volatile boolean isRunning = true;
    private Integer loopbackIndex = null;
    private static JsonTableSource parentTable;
    
    public ActorSource(JsonTableSource parentTableParam) {
        super();
        parentTable = parentTableParam;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Assumption: The number of Sources must be greater or equal to the number of Sinks on the same JVM.
        loopbackIndex = ActorSink.registerQueue.take();
        parentTable.registerChildFunction(this);
        
    }
    
    public void run(SourceContext<Row> sc) throws Exception {
        
        while (isRunning && !receivePort.isClosed()) {
            Row row = receivePort.receive();
            // par convention
            row.setField(0, "" + loopbackIndex);
            sc.collect(row);
        }
    }
    
    public void cancel() {
        isRunning = false;
    }
    
    public void emitRow(Row row) throws SuspendExecution, InterruptedException {
        channelToSource.send(row);
    }

}
