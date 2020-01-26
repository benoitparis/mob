package paris.benoit.mob.cluster;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.table.AppendStreamTableUtils;
import paris.benoit.mob.cluster.table.TemporalTableUtils;
import paris.benoit.mob.cluster.table.js.JsTableEngine;
import paris.benoit.mob.cluster.table.json.JsonTableSink;
import paris.benoit.mob.cluster.table.json.JsonTableSource;
import paris.benoit.mob.cluster.table.tick.TickTableSource;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;


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
        registerDataFlow();
        startFlink();
        
        configuration.underTowLauncher.waitUnderTowAvailable();
        waitRegistrationsReady();

        String plan = sEnv.getExecutionPlan();
        logger.info("Plan is: \n" + plan);
        logger.info("Front at: " + configuration.underTowLauncher.getUrl());
        logger.info("Web UI at: http://localhost:" + configuration.flinkWebUiPort);
        String[] tables = tEnv.listTables();
        logger.info("Tables are: " + Arrays.asList(tables));
        logger.info("Mob Cluster is up");
    }

    private void setupFlink() {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, configuration.flinkWebUiPort);
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        conf.setLong(MetricOptions.LATENCY_INTERVAL, configuration.latencyTrackingInterval);
        
        // ça passe en mode cluster, ça?
        sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        
        sEnv.setStreamTimeCharacteristic(configuration.processingtime);
        // TODO sort out setParallelism vs setMaxParallelism
        sEnv.setParallelism(configuration.streamParallelism);
        sEnv.setMaxParallelism(configuration.streamParallelism); // "It also defines the number of key groups used for partitioned state. "
        sEnv.setBufferTimeout(configuration.maxBufferTimeMillis);
        
        EnvironmentSettings bsSettings = 
            EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
            .build();
        tEnv = StreamTableEnvironment.create(sEnv, bsSettings);
        
    }

    private void registerInputOutputTables() {

        tEnv.registerTableSource("tick_service", new TickTableSource(20));
        
        for (MobTableConfiguration inSchema: configuration.inSchemas) {
            // wait for bug fix / understanding TableSource duplication
//            tEnv.registerTableSource(inSchema.name, new JsonTableSource(inSchema));

            AppendStreamTableUtils.createAndRegisterTableSourceDoMaterializeAsAppendStream(tEnv, new JsonTableSource(inSchema), inSchema.name);
        }

        for (MobTableConfiguration outSchema: configuration.outSchemas) {
            tEnv.registerTableSink(outSchema.name, new JsonTableSink(outSchema));
            logger.debug("Registered Table Sink: " + outSchema);
        }
        
    }

    private void registerDataFlow() {

        for (MobTableConfiguration sqlConf: configuration.sql) {
            logger.debug("Adding " + sqlConf.name + " of type " + sqlConf.confType);
            try {
                if (null == sqlConf.confType) {
                    throw new RuntimeException("Configuration type required for " + sqlConf);
                }
                switch (sqlConf.confType) {
                    case TABLE:
                        tEnv.registerTable(sqlConf.name, tEnv.sqlQuery(sqlConf.content));
                        break;
                    case STATE:
                        TemporalTableUtils.createAndRegister(tEnv, sqlConf);
                        break;
                    case JS_ENGINE:
                        JsTableEngine.createAndRegister(tEnv, sqlConf, configuration);
                        break;
                    case UPDATE:
                        // TODO wait for detailed Row schema printing
                        tEnv.sqlUpdate(sqlConf.content);

                        break;
                        default:
                            throw new RuntimeException("No SQL type was specified");
                }

                logger.info("Tables are: " + Arrays.asList(tEnv.listTables()));

            }
            catch (Throwable t) {
                throw new RuntimeException("" + sqlConf, t);
            }
        }

    }
    
    private void startFlink() {
        new Thread(() -> {
            try {
                // Blocking until cancellation
                sEnv.execute();
                logger.info("Stream END");
            } catch (Exception e) {
                logger.error("Stream execution failure", e);
            }
        }).start();
    }


    public static class NameSenderPair {
        String name;
        Integer loopbackIndex;
        MobClusterSender sender;
        NameSenderPair(String name, Integer loopbackIndex, MobClusterSender sender) {
            this.name = name;
            this.loopbackIndex = loopbackIndex;
            this.sender = sender;
        }
        public Integer getLoopbackIndex() {
            return loopbackIndex;
        }
        @Override
        public String toString() {
            return "NameSenderPair{" + "name='" + name + '\'' + ", loopbackIndex=" + loopbackIndex + ", sender=" + sender + '}';
        }
    }
    private static CopyOnWriteArrayList<NameSenderPair> clusterSenderRaw = new CopyOnWriteArrayList<>();
    private static List<Map<String, MobClusterSender>> clusterSenders = new ArrayList<>();
    public static void registerClusterSender(String tableName, MobClusterSender sender, Integer loopbackIndex) {
        clusterSenderRaw.add(new NameSenderPair(tableName, loopbackIndex, sender));
    }

    private static volatile boolean registrationDone = false;
    private static final int POLL_INTERVAL = 1000;
    private void waitRegistrationsReady() throws InterruptedException {
        int parallelism = sEnv.getParallelism();
        // On attend que tous les senders soient là
        // FIXME idéalement on fait que react à quand c'est prêt
        while ((clusterSenderRaw.size() != parallelism * configuration.inSchemas.size())
                || !JsTableEngine.isReady()
        ) {
            logger.info("Waiting to receive all senders: " + clusterSenderRaw.size() + " != " + parallelism * configuration.inSchemas.size() + " and JsTableEngines");
            logger.info("" + clusterSenderRaw);
            //logger.info("Plan is: \n" + sEnv.getExecutionPlan());
            Thread.sleep(POLL_INTERVAL);
        }
        doClusterSendersMatching(parallelism);
    }

    private void doClusterSendersMatching(int parallelism) {

        Map<String, List<NameSenderPair>> byName = clusterSenderRaw.stream()
            .sorted(Comparator.comparing(NameSenderPair::getLoopbackIndex))
            .collect(
            Collectors.groupingBy(
                it -> it.name,
                Collectors.toList()
            )
        );
        
        logger.info("Cluster senders names: " + byName.keySet());
        
        for (int i = 0; i < parallelism; i++) {
            HashMap<String, MobClusterSender> localMap = new HashMap<>();
            for(MobTableConfiguration ci: configuration.inSchemas) {
                localMap.put(ci.name, byName.get(ci.name).get(i).sender);
            }
            clusterSenders.add(localMap);
        }
                
        registrationDone = true;
        logger.info("Registration Done");
    }

    public static Map<String, MobClusterSender> getClusterSender(String random) throws InterruptedException {
        while (!registrationDone) {
            logger.info("Waiting on registration");
            Thread.sleep(POLL_INTERVAL);
        }
        
        return clusterSenders.get(Math.abs(random.hashCode()) % clusterSenders.size());
    }

}
