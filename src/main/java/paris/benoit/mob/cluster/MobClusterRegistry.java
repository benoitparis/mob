package paris.benoit.mob.cluster;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.cluster.MobClusterConfiguration.ConfigurationItem;
import paris.benoit.mob.cluster.loopback.ActorSource;
import paris.benoit.mob.cluster.table.AppendStreamTableUtils;
import paris.benoit.mob.cluster.table.TemporalTableUtils;
import paris.benoit.mob.cluster.table.json.JsonTableSink;
import paris.benoit.mob.cluster.table.json.JsonTableSource;

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
    public static JsonRowDeserializationSchema jrds;
    public void registerInputOutputTables() throws IOException {
        
        for (ConfigurationItem inSchema: configuration.inSchemas) {
            JsonTableSource tableSource = AppendStreamTableUtils.createAndRegisterTableSource(tEnv, inSchema);
            jrds = tableSource.getJsonRowDeserializationSchema();
        }

        for (ConfigurationItem outSchema: configuration.outSchemas) {
            tEnv.registerTableSink(outSchema.name, new JsonTableSink(outSchema.content));
        }
        
    }

    public void registerIntermediateTables() throws IOException {

        for (ConfigurationItem state: configuration.states) {
            TemporalTableUtils.createAndRegister(tEnv, state);
        }

        for (ConfigurationItem query: configuration.queries) {
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
    private static CopyOnWriteArrayList<MobClusterSender> clusterSenders = new CopyOnWriteArrayList<MobClusterSender>();
    public static MobClusterSender registerSourceFunction(ActorSource function) throws InterruptedException {

        // comment associer jrds lié à la table source?
        final MobClusterSender sender = new MobClusterSender(jrds);
        clusterSenders.add(sender);
        return sender;
        
    }

    private static volatile boolean veawingDone = false;
    private static final int POLL_INTERVAL = 1000;
    private void waitSourcesAndSinksRegistered() throws InterruptedException {
        int parallelism = sEnv.getParallelism();
        // On attend que Sources soient là
        while (clusterSenders.size() != parallelism) {
            Thread.sleep(POLL_INTERVAL);
            logger.info("Waiting on sources and sinks: " + parallelism + " " + clusterSenders.size());
        };
        
        veawingDone = true;
        logger.info("Weaving Done");
    }

    public static MobClusterSender getClusterSender(String random) throws InterruptedException {
        while (false == veawingDone) {
            Thread.sleep(POLL_INTERVAL);
            logger.info("Waiting on weaving");
        };
        
        return clusterSenders.get(Math.abs(random.hashCode()) % clusterSenders.size());
    }

}
