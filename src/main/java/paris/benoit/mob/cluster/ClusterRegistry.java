package paris.benoit.mob.cluster;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.cluster.json2sql.ClusterSender;
import paris.benoit.mob.cluster.json2sql.JsonTableSink;
import paris.benoit.mob.cluster.json2sql.JsonTableSource;
import paris.benoit.mob.cluster.json2sql.NumberedReceivePort;
import paris.benoit.mob.cluster.loopback.ActorSink;
import paris.benoit.mob.cluster.loopback.ActorSource;

public class ClusterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ClusterRegistry.class);
    
    private static final int POLL_INTERVAL = 1000;
    
    private volatile int parallelism;
    private static AtomicLong sinkCount = new AtomicLong();
    private static AtomicLong sourceCount = new AtomicLong();
    
    // Mono Input pour le moment
    private static JsonRowDeserializationSchema jrds;
    
    private StreamExecutionEnvironment sEnv;
    private StreamTableEnvironment tEnv;
    private String inSchema;
    private String inName;
    private String outSchema;
    private String outName;
    private String[] states;
    private String[] queries;
    
    public ClusterRegistry(
            StreamExecutionEnvironment sEnv, 
            StreamTableEnvironment tEnv, 
            String inSchema, String inName,
            String outSchema, String outName,
            String[] states, String[] queries
        ) {
        super();
        this.sEnv = sEnv;
        this.tEnv = tEnv;
        this.inSchema = inSchema;
        this.inName = inName;
        this.outSchema = outSchema;
        this.outName = outName;
        this.states = states;
        this.queries = queries;
        
        parallelism = sEnv.getParallelism();
    }
    
    public void registerInputOutputTables() throws IOException {
        
        final JsonTableSource tableSource = new JsonTableSource(inSchema);
        jrds = tableSource.getJsonRowDeserializationSchema();
        tEnv.registerTableSource(inName + "_raw", tableSource);
        Table hashInputTable = tEnv.sqlQuery(
            "SELECT\n" + 
            "  " + StringUtils.join(tableSource.getTableSchema().getFieldNames(), ",\n  ") + "\n" +
            "FROM " + inName + "_raw" + " \n"
        );
        DataStream<Row> appendStream = tEnv
            .toAppendStream(hashInputTable, tableSource.getReturnType());
        tEnv.registerTable(inName, tEnv.fromDataStream(appendStream, 
                StringUtils.join(tableSource.getTableSchema().getFieldNames(), ", ") +
                ", proctime.proctime")
        );
        
        
        tEnv.registerTableSink(outName, new JsonTableSink(outSchema));
    }


    public static final Pattern TEMPORAL_TABLE_PATTERN = Pattern.compile(
        "CREATE TEMPORAL TABLE ([^ ]+) TIME ATTRIBUTE ([^ ]+) PRIMARY KEY ([^ ]+) AS(.*)",
        Pattern.DOTALL
    );
    public void registerIntermediateTables() throws IOException {

        for (String state: states) {
            Matcher m = TEMPORAL_TABLE_PATTERN.matcher(state);
            if (m.matches()) {
                Table historyTable = tEnv.sqlQuery(m.group(4));
                TemporalTableFunction temporalTable = historyTable.createTemporalTableFunction(m.group(2), m.group(3));
                tEnv.registerFunction(m.group(1), temporalTable);
            } else {
                logger.debug("Failed to create temporal table with: \n" + state);
            }
        }

        for (String query: queries) {
            tEnv.sqlUpdate(query);
        }
        
    }
    
    private static ArrayBlockingQueue<Integer> sinkSourceQueue = new ArrayBlockingQueue<Integer>(1000);
    private static CopyOnWriteArrayList<ClusterSender> clusterSenders = new CopyOnWriteArrayList<ClusterSender>();
    public static Integer registerSinkFunction(ActorSink function) throws InterruptedException {
        int index = function.getRuntimeContext().getIndexOfThisSubtask();
        sinkCount.incrementAndGet();
        sinkSourceQueue.put(index);
        logger.info("Registered Sink #" + index);
        return index;
    }
    public static NumberedReceivePort<Row> registerSourceFunction(ActorSource function) throws InterruptedException {
        
        Channel<Row> channel = Channels.newChannel(1000000, OverflowPolicy.BACKOFF, false, false);
        ThreadReceivePort<Row> receivePort = new ThreadReceivePort<Row>(channel);

        // register dans lea clusterSenders avec un enum WRITE?
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
        
        registerInputOutputTables();
        registerIntermediateTables();
        
        new Thread(() -> {
            try {
                logger.info("Stream is being initialized. Execution plan: \n"+ sEnv.getExecutionPlan());
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
