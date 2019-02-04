package paris.benoit.mob.cluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;
import paris.benoit.mob.cluster.json2sql.ClusterSender;
import paris.benoit.mob.cluster.json2sql.JsonTableSink;
import paris.benoit.mob.cluster.json2sql.JsonTableSource;
import paris.benoit.mob.cluster.json2sql.NumberedReceivePort;
import paris.benoit.mob.cluster.loopback.ActorSink;
import paris.benoit.mob.cluster.loopback.ActorSource;

public class RegistryWeaver {
    
    private static final int POLL_INTERVAL = 1000;
    
    private volatile int parallelism;
    private static AtomicLong sinkCount = new AtomicLong();
    private static AtomicLong sourceCount = new AtomicLong();
    
    // Mono Input pour le moment
    private static JsonRowDeserializationSchema jrds;
    
    private StreamExecutionEnvironment sEnv;
    private Path in;
    private Path out;
    private Path inBetween;
    
    public RegistryWeaver(StreamExecutionEnvironment sEnv, Path in, Path out, Path inBetween) {
        super();
        this.sEnv = sEnv;
        this.in = in;
        this.out = out;
        this.inBetween = inBetween;
    }
    
    private void setUpTables() throws IOException {

        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(sEnv);
        String inSchema = new String(Files.readAllBytes(in));
        final JsonTableSource tableSource = new JsonTableSource(inSchema);
        jrds = tableSource.getJsonRowDeserializationSchema();
        tEnv.registerTableSource("inputTable", tableSource);
        String outSchema = new String(Files.readAllBytes(out));
        tEnv.registerTableSink("outputTable", new JsonTableSink(outSchema));
        String stringSQL = new String(Files.readAllBytes(inBetween));
        tEnv.sqlUpdate(stringSQL);
        
        parallelism = sEnv.getParallelism();
    }
    
    private static ArrayBlockingQueue<Integer> sinkSourceQueue = new ArrayBlockingQueue<Integer>(1000);
    private static CopyOnWriteArrayList<ClusterSender> clusterSenders = new CopyOnWriteArrayList<ClusterSender>();
    public static Integer registerSink(ActorSink function) throws InterruptedException {
        int index = function.getRuntimeContext().getIndexOfThisSubtask();
        sinkCount.incrementAndGet();
        sinkSourceQueue.put(index);
        System.out.println("Registered Sink #" + index);
        return index;
    }
    public static NumberedReceivePort<Row> registerSource(ActorSource function) throws InterruptedException {
        
        Channel<Row> channel = Channels.newChannel(1000000, OverflowPolicy.BACKOFF, false, false);
        ThreadReceivePort<Row> receivePort = new ThreadReceivePort<Row>(channel);
        
        clusterSenders.add(new ClusterSender(channel, jrds)); 
        sourceCount.incrementAndGet();
        Integer index = sinkSourceQueue.take();

        System.out.println("Registered Source #" + index);
        return new NumberedReceivePort<Row>(receivePort, index);
    }
    
    private static volatile boolean veawingDone = false;
    public void weaveComponents() throws InterruptedException, IOException {
        
        setUpTables();
        
        new Thread(() -> {
            try {
                System.out.println("Stream is being initialized. Execution plan: \n"+ sEnv.getExecutionPlan());
                sEnv.execute();
                // Launch
                // Blocking until cancellation
                sEnv.execute();
                System.out.println("Stream END");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        
        
        // On attends que Sources et Sinks se trouvent
        while (sinkCount.get() != (long) parallelism 
            || sourceCount.get()  != (long) parallelism 
            || sinkSourceQueue.size() != 0 
            || clusterSenders.size() != parallelism) {
            Thread.sleep(POLL_INTERVAL);
            System.out.println("Waiting on sources and sinks: " + parallelism + " " + sinkCount + " " + sourceCount + " " + clusterSenders.size() + " " + sinkSourceQueue.size() + " ");
        };
        
        System.out.println("Weaving Done");
        
        veawingDone = true;
    }
    
    public static ClusterSender getClusterSender(String random) throws InterruptedException {
        while (false == veawingDone) {
            Thread.sleep(POLL_INTERVAL);
            System.out.println("Waiting on weaving");
        };
        return clusterSenders.get(random.hashCode() % clusterSenders.size());
    }


}
