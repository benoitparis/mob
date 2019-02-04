package paris.benoit.mob.loopback;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.message.ClusterMessage;

@SuppressWarnings("serial")
public class ActorEntrySource extends RichParallelSourceFunction<Tuple2<Integer, ClusterMessage>> {
    
    private static Channel<ClusterMessage> channelToSource = Channels.newChannel(1000000, OverflowPolicy.BACKOFF, false, false);
    private static ThreadReceivePort<ClusterMessage> receivePort = new ThreadReceivePort<ClusterMessage>(channelToSource);

    private volatile boolean isRunning = true;
    private Integer backChannel = null;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Assumption: The number of Sources must be greater or equal to the number of Sinks on the same JVM.
        backChannel = ActorLoopBackSink.registerQueue.take();
    }
    
    public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Tuple2<Integer, ClusterMessage>> ctx) throws Exception {
        while (isRunning && !receivePort.isClosed()) {
            ctx.collect(Tuple2.of(backChannel, receivePort.receive()));
        }
    }
    
    public void cancel() {
        isRunning = false;
    }
    
    public static void send(ClusterMessage message) throws SuspendExecution, InterruptedException {
        channelToSource.send(message);
    }

}
