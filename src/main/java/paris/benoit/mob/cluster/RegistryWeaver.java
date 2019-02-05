package paris.benoit.mob.cluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.plan.logical.LogicalRelNode;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(RegistryWeaver.class);
    
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
        
        //chopper le plan, et le schema du payload:
        
//        FlinkPlannerImpl planner = new FlinkPlannerImpl(tEnv.getFrameworkConfig(), tEnv.getPlanner(), tEnv.getTypeFactory());
//        SqlInsert insert = (SqlInsert) planner.parse(stringSQL);
//        SqlNode validatedQuery = planner.validate(insert.getSource());
//        Table queryResult = new Table(tEnv, new LogicalRelNode(planner.rel(validatedQuery).rel));
//        System.out.println(queryResult);
//        // c'était un SQL timestamp
//        queryResult.logicalPlan().output();
        
        
        tEnv.sqlUpdate(stringSQL);
        
        parallelism = sEnv.getParallelism();
    }
    
    private static ArrayBlockingQueue<Integer> sinkSourceQueue = new ArrayBlockingQueue<Integer>(1000);
    private static CopyOnWriteArrayList<ClusterSender> clusterSenders = new CopyOnWriteArrayList<ClusterSender>();
    public static Integer registerSink(ActorSink function) throws InterruptedException {
        int index = function.getRuntimeContext().getIndexOfThisSubtask();
        sinkCount.incrementAndGet();
        sinkSourceQueue.put(index);
        logger.info("Registered Sink #" + index);
        return index;
    }
    public static NumberedReceivePort<Row> registerSource(ActorSource function) throws InterruptedException {
        
        Channel<Row> channel = Channels.newChannel(1000000, OverflowPolicy.BACKOFF, false, false);
        ThreadReceivePort<Row> receivePort = new ThreadReceivePort<Row>(channel);
        
        clusterSenders.add(new ClusterSender(channel, jrds)); 
        sourceCount.incrementAndGet();
        // simplifier le take? et juste valider le in et out même niveau de parallelism?
        //   juste le getIndex?
        Integer index = sinkSourceQueue.take();

        logger.info("Registered Source #" + index);
        return new NumberedReceivePort<Row>(receivePort, index);
    }
    
    private static volatile boolean veawingDone = false;
    public void weaveComponents() throws InterruptedException, IOException {
        
        setUpTables();
        
        new Thread(() -> {
            try {
                logger.info("Stream is being initialized. Execution plan: \n"+ sEnv.getExecutionPlan());
                sEnv.execute();
                // Launch
                // Blocking until cancellation
                sEnv.execute();
                logger.info("Stream END");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        
        // On attend que Sources et Sinks se trouvent
        while (sinkCount.get() != (long) parallelism 
            || sourceCount.get() != (long) parallelism 
            || sinkSourceQueue.size() != 0 
            || clusterSenders.size() != parallelism) {
            Thread.sleep(POLL_INTERVAL);
            logger.info("Waiting on sources and sinks: " + parallelism + " " + sinkCount + " " + sourceCount + " " + clusterSenders.size() + " " + sinkSourceQueue.size() + " ");
        };
        
        logger.info("Weaving Done");
        
        veawingDone = true;
    }
    
    public static ClusterSender getClusterSender(String random) throws InterruptedException {
        while (false == veawingDone) {
            Thread.sleep(POLL_INTERVAL);
            logger.info("Waiting on weaving");
        };
        
        return clusterSenders.get(Math.abs(random.hashCode()) % clusterSenders.size());
    }

}
