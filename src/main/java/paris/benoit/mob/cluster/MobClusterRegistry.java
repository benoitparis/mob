package paris.benoit.mob.cluster;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.cluster.MobClusterConfiguration.ConfigurationItem;
import paris.benoit.mob.cluster.table.AppendStreamTableUtils;
import paris.benoit.mob.cluster.table.TemporalTableUtils;
import paris.benoit.mob.cluster.table.json.JsonTableSink;

public class MobClusterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(MobClusterRegistry.class);

    private MobClusterConfiguration configuration;
    private StreamExecutionEnvironment sEnv;
    private StreamTableEnvironment tEnv;
    
    public MobClusterRegistry(MobClusterConfiguration clusterConfiguration) {
        this.configuration = clusterConfiguration;
    }
    
    public void start() throws Exception {

        configuration.underTowLauncher.launchUntertow(configuration.name);
        setupFlink();
        registerInputOutputTables();
        registerIntermediateTables();
        startFlink();
        
        configuration.underTowLauncher.waitUnderTowAvailable();
        waitSendersRegistration();
        
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

    public void registerInputOutputTables() throws IOException {
        
        for (ConfigurationItem inSchema: configuration.inSchemas) {
            AppendStreamTableUtils.createAndRegisterTableSource(tEnv, inSchema);
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
    // TODO déplacer dans un ActorSources.register quand on aura du multi-input?
    //   avec du ActorSources.getClusterSender as well (on garde clsutersender en tant que tel)
    //     et on enlève le nom registry de cette classe
    //     et avec du ActorSources.waitSourcesRegistered (qui obersera d'abord combien de types de sources il doit recevoir)
    //    ActorSources, ou bien Sources, ou bien (Json?)TableSources
    private static CopyOnWriteArrayList<MobClusterSender> clusterSenders = new CopyOnWriteArrayList<MobClusterSender>();
    public static void registerClusterSender(MobClusterSender sender) throws InterruptedException {
        clusterSenders.add(sender);
    }

    private static volatile boolean registrationDone = false;
    private static final int POLL_INTERVAL = 1000;
    private void waitSendersRegistration() throws InterruptedException {
        int parallelism = sEnv.getParallelism();
        // On attend que tous les senders soient là
        while (clusterSenders.size() != parallelism) {
            logger.info("Waiting to receive all senders: " + parallelism + " != " + clusterSenders.size());
            Thread.sleep(POLL_INTERVAL);
        };
        
        registrationDone = true;
        logger.info("Registration Done");
    }

    public static MobClusterSender getClusterSender(String random) throws InterruptedException {
        while (false == registrationDone) {
            logger.info("Waiting on registration");
            Thread.sleep(POLL_INTERVAL);
        };
        
        return clusterSenders.get(Math.abs(random.hashCode()) % clusterSenders.size());
    }

}
