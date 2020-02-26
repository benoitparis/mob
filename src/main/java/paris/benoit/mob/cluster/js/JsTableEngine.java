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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
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

            MobTableConfiguration sinkConf = new MobTableConfiguration(tableConf.dbName, tableConf.name + "_in", inSchema, null);
            JsTableSink sink = new JsTableSink(tableConf, sinkConf, invokeFunction, sourceCode);

            MobTableConfiguration sourceConf = new MobTableConfiguration(tableConf.dbName, tableConf.name + "_out", outSchema, null);
            JsTableSource source = new JsTableSource(tableConf, sourceConf);

            catalog.createTable(
                    sourceConf.getObjectPath(),
                    ConnectorCatalogTable.source(source, false),
                    false
            );
            catalog.createTable(
                    sinkConf.getObjectPath(),
                    ConnectorCatalogTable.sink(sink, false),
                    false
            );

        } else {
            throw new RuntimeException("Failed to create js table. They must conform to: " + JS_TABLE_PATTERN_REGEX + "\nSQL was: \n" + tableConf);
        }
    }


    /*
     * We need to have the sink -> js engine -> source data path, we'll do it through a queue.
     * A queue cannot be set up through constructors, because of serialization (and remote execution)
     * Hopefully we have arranged for Co-Location though, and when functions are deserialized,
     * their will open() and exchange a queue.
     */

    private static TransferMap<String, BlockingQueue<Map>> transferMap = new TransferMap<>();

    static CompletableFuture<BlockingQueue<Map>> registerSource(String name) {
        return CompletableFuture.supplyAsync(() -> transferMap.getAndWait(name));
    }

    static void registerSink(String name, BlockingQueue<Map> queue) {
        transferMap.put(name, queue);
    }

    // https://stackoverflow.com/questions/6389122/does-a-hashmap-with-a-getandwait-method-exist-e-g-a-blockingconcurrenthashma
    public static class TransferMap<K, V> extends HashMap<K, V>{

        private final Object lock = new Object();

        V getAndWait(Object key) {
            V value;
            synchronized(lock) {
                do {
                    value = super.get(key);
                    if (value == null) {
                        try {
                            lock.wait();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } while(value == null);
            }
            return value;
        }

        @Override
        public V put(K key, V value){
            synchronized(lock) {
                super.put(key, value);
                lock.notifyAll();
            }
            return value;
        }

    }
}
