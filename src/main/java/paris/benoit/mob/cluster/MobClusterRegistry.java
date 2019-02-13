package paris.benoit.mob.cluster;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import co.paralleluniverse.strands.channels.Channels.OverflowPolicy;
import co.paralleluniverse.strands.channels.ThreadReceivePort;
import paris.benoit.mob.cluster.MobClusterConfiguration.ConfigurationItem;
import paris.benoit.mob.cluster.json2sql.JsonTableSink;
import paris.benoit.mob.cluster.json2sql.JsonTableSource;
import paris.benoit.mob.cluster.loopback.ActorSink;
import paris.benoit.mob.cluster.loopback.ActorSource;

// TODO éviter la god class: mettre tous les static nécessaires ici?
//   ou bien dans le package: pareil que UnderTow, un objet static ClusterMob
public class MobClusterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(MobClusterRegistry.class);

    private MobClusterConfiguration configuration;
    private StreamExecutionEnvironment sEnv;
    private StreamTableEnvironment tEnv;
    
    public MobClusterRegistry(MobClusterConfiguration clusterConfiguration) {
        this.configuration = clusterConfiguration;
    }
    
    public void weaveComponentsAndStart() throws Exception {

        configuration.underTowLauncher.launchUntertow(configuration.name);
        setupFlink();
        registerInputOutputTables();
        registerIntermediateTables();
        startFlink();
        
        configuration.underTowLauncher.waitUnderTowAvailable();
        waitSourcesAndSinksRegistered();
        
        logger.info("Mob Cluster is up");
        logger.info("Front at: " + configuration.underTowLauncher.getUrl());
        logger.info("Web UI at: http://localhost:" + configuration.flinkWebUiPort);
        logger.info("Plan is: \n" + sEnv.getExecutionPlan());
    }
    
    private void setupFlink() {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, configuration.flinkWebUiPort);
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        // ça passe en mode cluster, ça?
        sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        
        sEnv.setStreamTimeCharacteristic(configuration.processingtime);
        sEnv.setParallelism(configuration.streamParallelism);
        sEnv.setBufferTimeout(configuration.maxBufferTimeMillis);
        tEnv = TableEnvironment.getTableEnvironment(sEnv);
    }

    // Mono Input pour le moment
    private static JsonRowDeserializationSchema jrds;
    public void registerInputOutputTables() throws IOException {
        
        for (ConfigurationItem inSchema: configuration.inSchemas) {
            final JsonTableSource tableSource = new JsonTableSource(inSchema.content);
            jrds = tableSource.getJsonRowDeserializationSchema();
            tEnv.registerTableSource(inSchema.name + "_raw", tableSource);
            Table hashInputTable = tEnv.sqlQuery(
                "SELECT\n" + 
                "  " + StringUtils.join(tableSource.getTableSchema().getFieldNames(), ",\n  ") + "\n" +
                "FROM " + inSchema.name + "_raw" + " \n"
            );
            DataStream<Row> appendStream = tEnv
                .toAppendStream(hashInputTable, tableSource.getReturnType());
            tEnv.registerTable(inSchema.name, tEnv.fromDataStream(appendStream, 
                StringUtils.join(tableSource.getTableSchema().getFieldNames(), ", ") +
                ", proctime.proctime")
            );
        }

        for (ConfigurationItem outSchema: configuration.outSchemas) {
            tEnv.registerTableSink(outSchema.name, new JsonTableSink(outSchema.content));
        }
    }


    public static final Pattern TEMPORAL_TABLE_PATTERN = Pattern.compile(
        "CREATE TEMPORAL TABLE ([^ ]+) TIME ATTRIBUTE ([^ ]+) PRIMARY KEY ([^ ]+) AS(.*)",
        Pattern.DOTALL
    );
    public void registerIntermediateTables() throws IOException {

        for (ConfigurationItem state: configuration.states) {
            Matcher m = TEMPORAL_TABLE_PATTERN.matcher(state.content);
            
            if (m.matches()) {
                Table historyTable = tEnv.sqlQuery(m.group(4));
                TemporalTableFunction temporalTable = historyTable.createTemporalTableFunction(m.group(2), m.group(3));
                tEnv.registerFunction(m.group(1), temporalTable);
            } else {
                // TODO work on exception types?
                throw new RuntimeException("Failed to create temporal table with: \n" + state);
            }
            
            if (!m.group(1).trim().equalsIgnoreCase(state.name.trim())) {
                throw new RuntimeException("Created table must match with file name");
            }
        }

        for (ConfigurationItem query: configuration.queries) {
            // TODO check name?
            tEnv.sqlUpdate(query.content);
        }
    }
    
    private void startFlink() {
        new Thread(() -> {
            try {
                // Blocking until cancellation
                sEnv.execute();
                logger.info("Stream END");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
    
    // TODO static à enlever quand on pourra injecter dans l'actor
    //   @FiberSpringBootApplication
    //   Spring too big? Pas pour now
    private static ArrayBlockingQueue<Integer> sinkSourceQueue = new ArrayBlockingQueue<Integer>(1000);
    private static CopyOnWriteArrayList<MobClusterSender> clusterSenders = new CopyOnWriteArrayList<MobClusterSender>();
    private static AtomicLong sinkCount = new AtomicLong();
    public static Integer registerSinkFunction(ActorSink function) throws InterruptedException {
        int index = function.getRuntimeContext().getIndexOfThisSubtask();
        sinkCount.incrementAndGet();
        sinkSourceQueue.put(index);
        logger.info("Registered Sink #" + index);
        return index;
    }

    private static AtomicLong sourceCount = new AtomicLong();
    public static NumberedReceivePort<Row> registerSourceFunction(ActorSource function) throws InterruptedException {
        
        Channel<Row> channel = Channels.newChannel(1000000, OverflowPolicy.BACKOFF, false, false);
        ThreadReceivePort<Row> receivePort = new ThreadReceivePort<Row>(channel);

        // register dans lea clusterSenders avec un enum WRITE?
        clusterSenders.add(new MobClusterSender(channel, jrds)); 
        sourceCount.incrementAndGet();
        // simplifier le take? et juste valider le in et out même niveau de parallelism?
        //   juste le getIndex?
        Integer index = sinkSourceQueue.take();

        logger.info("Registered Source #" + index);
        return new NumberedReceivePort<Row>(receivePort, index);
    }

    private static volatile boolean veawingDone = false;
    private static final int POLL_INTERVAL = 1000;
    private void waitSourcesAndSinksRegistered() throws InterruptedException {
        int parallelism = sEnv.getParallelism();
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

    public static MobClusterSender getClusterSender(String random) throws InterruptedException {
        while (false == veawingDone) {
            Thread.sleep(POLL_INTERVAL);
            logger.info("Waiting on weaving");
        };
        
        return clusterSenders.get(Math.abs(random.hashCode()) % clusterSenders.size());
    }

}
