package paris.benoit.mob.cluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.external.js.ExternalJsEngine;
import paris.benoit.mob.cluster.loopback.GlobalClusterSenderRegistry;
import paris.benoit.mob.cluster.loopback.distributed.KafkaSchemaRegistry;
import paris.benoit.mob.cluster.services.DebugTableSink;
import paris.benoit.mob.cluster.services.DirectoryTableSource;
import paris.benoit.mob.cluster.services.TickTableSource;
import paris.benoit.mob.cluster.services.TwitterTableSink;
import paris.benoit.mob.cluster.utils.RetractStreamTableUtils;
import paris.benoit.mob.cluster.utils.TableSchemaConverter;
import paris.benoit.mob.cluster.utils.TemporalTableFunctionUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static paris.benoit.mob.cluster.utils.Colors.cyan;
import static paris.benoit.mob.cluster.utils.Colors.yellow;

public class MobCluster {
    private static final Logger logger = LoggerFactory.getLogger(MobCluster.class);

    private final MobClusterConfiguration configuration;
    private StreamExecutionEnvironment sEnv;
    private StreamTableEnvironment tEnv;
    private Catalog catalog;

    public MobCluster(MobClusterConfiguration clusterConfiguration) {
        this.configuration = clusterConfiguration;
    }


    public void start() throws Exception {
        // TODO: have a better startup sequence dependencies understanding / story
        //   not obvious:
        //   - ClusterRegistry.setConf before configuration.clusterFront.start, because ClientSimulators will fail otherwise
        //   - sEnv executed before configuration.clusterFront.waitReady, otherwise actors fail?
        //   - sEnv.getExecutionPlan() before sEnv.executeAsync(), because execution will clear the internal StreamGraph
        //   obvious:
        //   - have an env > service > apps > exec > info
        //   - for each app: IO > DataFlow
        setupEnvironment();
        configuration.clusterSenderRegistry.setConf(configuration);
        GlobalClusterSenderRegistry.setConf(configuration);
        configuration.clusterFront.configure(configuration);

        configuration.clusterFront.start();
        registerServiceTables();

        for(MobAppConfiguration app : configuration.apps) {
            catalog.createDatabase(app.name, new CatalogDatabaseImpl(new HashMap<>(), null), false);
            tEnv.useDatabase(app.name);
            registerDataFlow(app);
        }

//        String plan = sEnv.getExecutionPlan();
//        JobClient jobClient = sEnv.executeAsync();

        configuration.clusterFront.waitReady();

        List<String> tables = Arrays.stream(tEnv.listCatalogs())
                .flatMap(it -> {
                    tEnv.useCatalog(it);
                    return Arrays.stream(tEnv.listDatabases());
                })
                .flatMap(it -> {
                    tEnv.useDatabase(it);
                    return Arrays.stream(tEnv.listTables()).map(at -> it + "." + at);
                })
                .collect(Collectors.toList());
        String tablesString = tables.stream().collect(Collectors.joining(",\n ", "\n[\n ", "\n]"));

        KafkaSchemaRegistry.registerConfiguration(configuration);
        tables.forEach(name -> {
                // TODO enlever, toujours prendre tout le nom en entier
                KafkaSchemaRegistry.registerSchema("mobcatalog." + name, TableSchemaConverter.toJsonSchema(tEnv.from(name).getSchema()));
        });

        ExternalJsEngine.scanAndCreateJsEngine();

        configuration.clusterSenderRegistry.waitRegistrationsReady();

        logger.debug("Input schemas:\n" + KafkaSchemaRegistry.getInputSchemas());
        logger.debug("Output schemas:\n" + KafkaSchemaRegistry.getOutputSchemas());

//        logger.info("\n" + brightBlack(plan));
        logger.info("Plan ↑");

        logger.info("Tables: " + yellow(tablesString));
        logger.info("Front: " + yellow(configuration.clusterFront.accessString()));
        logger.info("Web UI: " + yellow("http://localhost:" + configuration.flinkWebUiPort));
        logger.info(cyan("Mob Cluster is up"));

        // TODO utiliser les StatementSet.execute, quand on aura le support des temporal table en DDL
        // TODO utiliser les StatementSet.execute, pour avoir le résultat du plan à la fin
        // TODO utiliser les StatementSet.execute, pour chopper le result, et chopper un schema directement, et le register dans la foulée?
        // TODO faire un StatementSet.execute, et le mettre pas à la fin

        tEnv.execute("job_name_TODO");
    }

    private void setupEnvironment() {

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, configuration.flinkWebUiPort);
        sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        // TODO mode de lancement en local, sinon par default en jar avec getExecutionEnvironment()

        sEnv.setParallelism(configuration.streamParallelism);
        sEnv.setMaxParallelism(configuration.streamParallelism);
        sEnv.setBufferTimeout(configuration.maxBufferTimeMillis);
        
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().build();
        tEnv = StreamTableEnvironment.create(sEnv, bsSettings);

        // TODO mettre "mobcatalog" en constante
        catalog = new GenericInMemoryCatalog("mobcatalog");
        tEnv.registerCatalog("mobcatalog", catalog);
        tEnv.useCatalog("mobcatalog");
    }

    private void registerServiceTables() throws TableAlreadyExistException, DatabaseNotExistException, DatabaseAlreadyExistException {
        catalog.createDatabase("services", new CatalogDatabaseImpl(new HashMap<>(), null), false);

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
        // TODO lister les services dont une app a besoin
        catalog.createTable(
                new ObjectPath("services", "twitter"),
                ConnectorCatalogTable.sink(new TwitterTableSink(), false),
                false
        );
    }

    private void registerDataFlow(MobAppConfiguration app) {


        for (MobTableConfiguration sqlConf: app.sql) {
            logger.debug("Adding " + sqlConf.name + " of type " + sqlConf.confType);
            try {
                if (null == sqlConf.confType) {
                    throw new RuntimeException("Configuration type required for " + sqlConf);
                }
                switch (sqlConf.confType) {
                    case STATE:
                        TemporalTableFunctionUtils.createAndRegister(tEnv, sqlConf);
                        break;
                    case RETRACT:
                        RetractStreamTableUtils.convertAndRegister(tEnv, sqlConf);
                        break;
                    case UPDATE:
                        // TODO refactor avec des statements, pour tout mettre en même temps, et que ça optimize
                        tEnv.sqlUpdate(sqlConf.content);
//                        tEnv.executeSql(sqlConf.content);
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
}
