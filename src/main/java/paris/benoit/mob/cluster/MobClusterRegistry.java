package paris.benoit.mob.cluster;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.table.AppendStreamTableUtils;
import paris.benoit.mob.cluster.table.RetractStreamTableUtils;
import paris.benoit.mob.cluster.table.TemporalTableFunctionUtils;
import paris.benoit.mob.cluster.table.services.DebugTableSink;
import paris.benoit.mob.cluster.table.js.JsTableEngine;
import paris.benoit.mob.cluster.table.json.JsonTableSink;
import paris.benoit.mob.cluster.table.json.JsonTableSource;
import paris.benoit.mob.cluster.table.services.DirectoryTableSource;
import paris.benoit.mob.cluster.table.services.TickTableSource;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class MobClusterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(MobClusterRegistry.class);

    private MobClusterConfiguration configuration;
    private StreamExecutionEnvironment sEnv;
    private StreamTableEnvironment tEnv;
    private Catalog catalog;

    public MobClusterRegistry(MobClusterConfiguration clusterConfiguration) {
        this.configuration = clusterConfiguration;
    }

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_YELLOW = "\u001B[33m";

    public void start() throws Exception {

        configuration.clusterFront.start();
        setupEnvironment();
        registerServiceTables();

        for(MobAppConfiguration app : configuration.apps) {
            catalog.createDatabase(app.name, new CatalogDatabaseImpl(new HashMap<String, String>(), null), false);
            tEnv.useDatabase(app.name);
            registerInputOutputTables(app);
            registerDataFlow(app);
        }

        executeEnvironment();
        configuration.clusterFront.waitReady();
        waitRegistrationsReady();

        String plan = sEnv.getExecutionPlan();
        logger.info(ANSI_YELLOW + "Plan is: \n" + ANSI_RESET + plan);
        logger.info("Front: " + ANSI_YELLOW + configuration.clusterFront.accessString() + ANSI_RESET);
        logger.info("Web UI: " + ANSI_YELLOW + "http://localhost:" + configuration.flinkWebUiPort + ANSI_RESET);
        String[] tables = tEnv.listTables();  // TODO lister tous les catalogs.db.tables
        logger.info("Tables: " + ANSI_YELLOW + Arrays.asList(tables) + ANSI_RESET);
        logger.info(ANSI_CYAN + "Mob Cluster is up" + ANSI_RESET);

    }

    private void setupEnvironment() {

        if (MobClusterConfiguration.ENV_MODE.LOCAL_UI.equals(configuration.mode)) {
            Configuration conf = new Configuration();
            conf.setInteger(RestOptions.PORT, configuration.flinkWebUiPort);
            conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
            sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else if (MobClusterConfiguration.ENV_MODE.LOCAL.equals(configuration.mode)) {
            sEnv = StreamExecutionEnvironment.createLocalEnvironment();
        } else if (MobClusterConfiguration.ENV_MODE.REMOTE.equals(configuration.mode)) {
            // TODO investigate jar and get vs remote
            sEnv = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 8081);
        }
        
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

        catalog = new GenericInMemoryCatalog("mobcatalog");
        tEnv.registerCatalog("mobcatalog", catalog);
        tEnv.useCatalog("mobcatalog");
    }

    private void registerServiceTables() throws TableAlreadyExistException, DatabaseNotExistException, DatabaseAlreadyExistException {
        catalog.createDatabase("services", new CatalogDatabaseImpl(new HashMap<String, String>(), null), false);

        catalog.createTable(
                new ObjectPath("services", "tick"),
                ConnectorCatalogTable.source(new TickTableSource(20), false),
                false
        );
        catalog.createTable(
                new ObjectPath("services", "debug"),
                ConnectorCatalogTable.sink(new DebugTableSink(), false),
                false
        );
        catalog.createTable(
                new ObjectPath("services", "app_list"),
                ConnectorCatalogTable.source(new DirectoryTableSource(configuration.apps), false),
                false
        );

    }

    private void registerInputOutputTables(MobAppConfiguration app) throws TableAlreadyExistException, DatabaseNotExistException {

        for (MobTableConfiguration inSchema: app.inSchemas) {
            // wait for bug fix / understanding TableSource duplication
//            tEnv.registerTableSource(inSchema.name, new JsonTableSource(inSchema));

            AppendStreamTableUtils.createAndRegisterTableSourceDoMaterializeAsAppendStream(app.name, tEnv, catalog, new JsonTableSource(inSchema), inSchema.name);
        }

        for (MobTableConfiguration outSchema: app.outSchemas) {

            catalog.createTable(
                    new ObjectPath(app.name, outSchema.name),
                    ConnectorCatalogTable.sink(new JsonTableSink(outSchema, configuration.router), false),
                    false
            );

            logger.debug("Registered Table Sink: " + outSchema);
        }
        
    }

    private void registerDataFlow(MobAppConfiguration app) throws DatabaseNotExistException {

        List<String> tables = catalog.listTables(app.name);
        logger.info("Tables are: " + tables);

        for (MobTableConfiguration sqlConf: app.sql) {
            logger.debug("Adding " + sqlConf.name + " of type " + sqlConf.confType);
            try {
                if (null == sqlConf.confType) {
                    throw new RuntimeException("Configuration type required for " + sqlConf);
                }
                switch (sqlConf.confType) {
                    case TABLE:
                        tEnv.createTemporaryView(sqlConf.fullyQualifiedName(), tEnv.sqlQuery(sqlConf.content));
                        break;
                    case STATE:
                        TemporalTableFunctionUtils.createAndRegister(tEnv, sqlConf);
                        break;
                    case RETRACT:
                        RetractStreamTableUtils.convertAndRegister(tEnv, sqlConf);
                        break;
                    case APPEND:
                        AppendStreamTableUtils.convertAndRegister(tEnv, sqlConf);
                        break;
                    case JS_ENGINE:
                        JsTableEngine.createAndRegister(catalog, tEnv, sqlConf);
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
    
    private void executeEnvironment() {
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
        String fullName;
        Integer loopbackIndex;
        MobClusterSender sender;
        NameSenderPair(String fullName, Integer loopbackIndex, MobClusterSender sender) {
            this.fullName = fullName;
            this.loopbackIndex = loopbackIndex;
            this.sender = sender;
        }
        public Integer getLoopbackIndex() {
            return loopbackIndex;
        }
        @Override
        public String toString() {
            return "NameSenderPair{" + "fullName='" + fullName + '\'' + ", loopbackIndex=" + loopbackIndex + ", sender=" + sender + '}';
        }
    }
    private static CopyOnWriteArrayList<NameSenderPair> clusterSenderRaw = new CopyOnWriteArrayList<>();
    private static List<Map<String, MobClusterSender>> clusterSenders = new ArrayList<>();
    public static void registerClusterSender(String fullName, MobClusterSender sender, Integer loopbackIndex) {
        clusterSenderRaw.add(new NameSenderPair(fullName, loopbackIndex, sender));
    }

    private static volatile boolean registrationDone = false;
    private static final int POLL_INTERVAL = 1000;
    private void waitRegistrationsReady() throws InterruptedException {
        int parallelism = sEnv.getParallelism();
        // On attend que tous les senders soient là
        // FIXME idéalement on fait que react à quand c'est prêt

        long inSchemaCount = configuration.apps.stream().flatMap(it -> it.inSchemas.stream()).count();

        while ((clusterSenderRaw.size() != parallelism * inSchemaCount)
                || !JsTableEngine.isReady()
        ) {
            logger.info("Waiting to receive all senders: " + clusterSenderRaw.size() + " != " + parallelism * inSchemaCount + " and JsTableEngines");
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
                it -> it.fullName,
                Collectors.toList()
            )
        );
        
        logger.info("Cluster senders names: " + byName.keySet());
        
        for (int i = 0; i < parallelism; i++) {
            HashMap<String, MobClusterSender> localMap = new HashMap<>();

            for(MobAppConfiguration app : configuration.apps) {
                for(MobTableConfiguration ci: app.inSchemas) {
                    localMap.put(ci.fullyQualifiedName(), byName.get(ci.fullyQualifiedName()).get(i).sender);
                }
                clusterSenders.add(localMap);
            }
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
