package paris.benoit.mob.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

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

import javax.script.ScriptException;

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
        registerJsEngines();
        registerInputOutputTables();
        registerDataFlow();
        startFlink();
        
        configuration.underTowLauncher.waitUnderTowAvailable();
        waitRegistrationsReady();
        
        logger.info("Mob Cluster is up");
        logger.info("Front at: " + configuration.underTowLauncher.getUrl());
        logger.info("Web UI at: http://localhost:" + configuration.flinkWebUiPort);
        logger.info("Tables are: " + Arrays.asList(tEnv.listTables()));
        logger.info("Plan is: \n" + sEnv.getExecutionPlan());
        
    }
    
    private void setupFlink() {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, configuration.flinkWebUiPort);
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        conf.setLong(MetricOptions.LATENCY_INTERVAL, configuration.latencyTrackingInterval);
        
        // ça passe en mode cluster, ça?
        sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        
        sEnv.setStreamTimeCharacteristic(configuration.processingtime);
        sEnv.setParallelism(configuration.streamParallelism);
        sEnv.setBufferTimeout(configuration.maxBufferTimeMillis);
        
        EnvironmentSettings bsSettings = 
            EnvironmentSettings.newInstance()
                .useOldPlanner()
//                .useBlinkPlanner()
                .inStreamingMode()
            .build();
        tEnv = StreamTableEnvironment.create(sEnv, bsSettings);
        
    }



    public void registerJsEngines() throws IOException, ScriptException {

        for (MobTableConfiguration conf: configuration.js) {
            JsTableEngine tableEngine = new JsTableEngine(conf);
            tEnv.registerTableSink(tableEngine.getSink().getName(), tableEngine.getSink());
            tEnv.registerTableSource(tableEngine.getSource().getName(), tableEngine.getSource());
        }

    }

    public void registerInputOutputTables() throws IOException {
        
        for (MobTableConfiguration inSchema: configuration.inSchemas) {
            AppendStreamTableUtils.createAndRegisterTableSource(tEnv, inSchema);
        }

        for (MobTableConfiguration outSchema: configuration.outSchemas) {
            tEnv.registerTableSink(outSchema.name, new JsonTableSink(outSchema));
        }
        
    }

    public void registerDataFlow() throws IOException {


        for (MobTableConfiguration sqlConf: configuration.sql) {
            try {
                switch (sqlConf.confType) {
                    case TABLE:
                        tEnv.registerTable(sqlConf.name, tEnv.sqlQuery(sqlConf.content));
                        break;
                    case STATE:
                        TemporalTableUtils.createAndRegister(tEnv, sqlConf);
                        break;
                    case UPDATE:
                        // TODO payload: Row
                        // PayloadedTableUtils.wrapPrettyErrorAndUpdate
                        // ou bien un mode où infer le out schema? yep, contrat d'interface good
                        tEnv.sqlUpdate(sqlConf.content);
                        break;
                        default:
                            throw new RuntimeException("No SQL type was specified");
                }

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
                e.printStackTrace();
            }
        }).start();
    }

    // TODO static à enlever quand on pourra injecter dans l'actor
    //   @FiberSpringBootApplication
    //   Spring too big? Pas pour now
    // TODO déplacer dans un ActorSources.register quand on aura du multi-input? 
    //          (et ça sera register d'un channel,nom)
    //   avec du ActorSources.getClusterSender as well (on garde clsutersender en tant que tel)
    //     et on enlève le nom registry de cette classe
    //     et avec du ActorSources.waitSourcesRegistered (qui obersera d'abord combien de types de sources il doit recevoir)
    //    ActorSources, ou bien Sources, ou bien (Json?)TableSources
    public static class NameSenderPair {
        public String name;
        public Integer loopbackIndex;
        public MobClusterSender sender;
        public NameSenderPair(String name, Integer loopbackIndex, MobClusterSender sender) {
            this.name = name;
            this.loopbackIndex = loopbackIndex;
            this.sender = sender;
        }
    }
    private static CopyOnWriteArrayList<NameSenderPair> clusterSenderRaw = new CopyOnWriteArrayList<NameSenderPair>();
    private static List<Map<String, MobClusterSender>> clusterSenders = new ArrayList<>();
    public static void registerClusterSender(String tableName, MobClusterSender sender, Integer loopbackIndex) throws InterruptedException {
        clusterSenderRaw.add(new NameSenderPair(tableName, loopbackIndex, sender));
    }

    private static volatile boolean registrationDone = false;
    private static final int POLL_INTERVAL = 1000;
    private void waitRegistrationsReady() throws InterruptedException {
        int parallelism = sEnv.getParallelism();
        // On attend que tous les senders soient là
        while ((clusterSenderRaw.size() != parallelism * configuration.inSchemas.size()) && !JsTableEngine.isReady()) {
            logger.info("Waiting to receive all senders: " + parallelism + " != " + clusterSenderRaw.size() + " and JsTableEngines");
            Thread.sleep(POLL_INTERVAL);
        };
        doClusterSendersMatching(parallelism);
    }

    private void doClusterSendersMatching(int parallelism) throws InterruptedException {

        Map<String, List<NameSenderPair>> byName = clusterSenderRaw.stream()
            .sorted((a, b) -> a.loopbackIndex > b.loopbackIndex ? -1 : 1)
            .collect(
            Collectors.groupingBy(
                it -> it.name,
                Collectors.toList()
            )
        );
        
        logger.info("Cluster senders names: " + byName.keySet());
        
        for (int i = 0; i < parallelism; i++) {
            HashMap<String, MobClusterSender> localMap = new HashMap<String, MobClusterSender>();
            for(MobTableConfiguration ci: configuration.inSchemas) {
                localMap.put(ci.name, byName.get(ci.name).get(i).sender);
            }
            clusterSenders.add(localMap);
        }
                
        registrationDone = true;
        logger.info("Registration Done");
    }

    public static Map<String, MobClusterSender> getClusterSender(String random) throws InterruptedException {
        while (false == registrationDone) {
            logger.info("Waiting on registration");
            Thread.sleep(POLL_INTERVAL);
        };
        
        return clusterSenders.get(Math.abs(random.hashCode()) % clusterSenders.size());
    }

}
