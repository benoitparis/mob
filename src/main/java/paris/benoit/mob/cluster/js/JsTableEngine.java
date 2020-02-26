package paris.benoit.mob.cluster.js;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsTableEngine {
    private static final Logger logger = LoggerFactory.getLogger(JsTableEngine.class);

    private static final String JS_TABLE_PATTERN_REGEX = "" +
            "CREATE TABLE ([^\\s]+)[\\s]+" +
            "LANGUAGE js[\\s]+" +
            "SOURCE '([^\\s]+)'[\\s]+" +
            "IN_SCHEMA '([^\\s]+)'[\\s]+" +
            "OUT_SCHEMA '([^\\s]+)'[\\s]+" +
            "INVOKE '([^\\s]+)'[\\s]*";
    private static final Pattern JS_TABLE_PATTERN = Pattern.compile(JS_TABLE_PATTERN_REGEX, Pattern.DOTALL);

    public static void createAndRegister(Catalog catalog, MobTableConfiguration tableConf) throws IOException, TableAlreadyExistException, DatabaseNotExistException {
        Matcher m = JS_TABLE_PATTERN.matcher(tableConf.content);

        if (m.matches()) {
            if (!m.group(1).trim().equalsIgnoreCase(tableConf.name.trim())) {
                throw new RuntimeException("Created table must match with file name");
            }

            // TODO move this
            String sourceCode = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/" + tableConf.dbName + "/" + m.group(2))));
            String inSchema = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/" + tableConf.dbName + "/" + m.group(3))));
            String outSchema = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/" + tableConf.dbName + "/" + m.group(4))));
            String invokeFunction = m.group(5);

            setupQueue(tableConf.name);

            JsTableSink sink = new JsTableSink(tableConf, new MobTableConfiguration(tableConf.dbName,tableConf.name + "_in", inSchema, null), invokeFunction, sourceCode);
            JsTableSource source = new JsTableSource(tableConf, new MobTableConfiguration(tableConf.dbName,tableConf.name + "_out", outSchema, null));

            catalog.createTable(
                    source.configuration.getObjectPath(), // TODO change
                    ConnectorCatalogTable.source(source, false),
                    false
            );
            catalog.createTable(
                    sink.configuration.getObjectPath(),
                    ConnectorCatalogTable.sink(sink, false),
                    false
            );

        } else {
            throw new RuntimeException("Failed to create js table. They must conform to: " + JS_TABLE_PATTERN_REGEX + "\nSQL was: \n" + tableConf);
        }
    }

    private static final Map<String, BlockingQueue<BlockingQueue<Map>>> queuesSink = new HashMap<>();
    private static final Map<String, BlockingQueue<BlockingQueue<Map>>> queuesSource = new HashMap<>();

    // TODO remove, useful??
    private static final AtomicInteger registrationTarget = new AtomicInteger(0);
    private static final AtomicInteger counter = new AtomicInteger(0);

    private static void setupQueue(String name) {
        BlockingQueue<BlockingQueue<Map>> exchangeQueueSink = new ArrayBlockingQueue<>(1);
        BlockingQueue<BlockingQueue<Map>> exchangeQueueSource = new ArrayBlockingQueue<>(1);
        queuesSink.put(name, exchangeQueueSink);
        queuesSource.put(name, exchangeQueueSource);

        BlockingQueue<Map> queue = new ArrayBlockingQueue<>(100);

        exchangeQueueSink.add(queue);
        exchangeQueueSource.add(queue);

        registrationTarget.addAndGet(2);

    }

    static BlockingQueue<Map> registerSink(String name) throws InterruptedException {
        logger.warn("Registering JS Sink function " + name);
        while (null == queuesSink.get(name)){
            logger.warn("Waiting on registerSink, this shouldn't happen");
            Thread.sleep(100);
        }
        counter.incrementAndGet();
        return queuesSink.get(name).take();
    }

    static BlockingQueue<Map> registerSource(String name) throws InterruptedException {
        logger.warn("Registering JS Source function " + name);
        while (null == queuesSource.get(name)){
            logger.warn("Waiting on registerSource, this shouldn't happen");
            Thread.sleep(100);
        }
        counter.incrementAndGet();
        return queuesSource.get(name).take();
    }

    public static boolean isReady() {
        logger.info("JsEngine Polled, registrationTarget = " + registrationTarget + ", counter = " + counter);
        return registrationTarget.get() == counter.get();
    }

}
