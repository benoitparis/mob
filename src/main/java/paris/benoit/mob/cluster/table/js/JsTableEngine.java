package paris.benoit.mob.cluster.table.js;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.table.AppendStreamTableUtils;

import javax.script.ScriptException;
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

    public static void createAndRegister(StreamTableEnvironment tEnv, MobTableConfiguration tableConf, MobClusterConfiguration clusterConf) throws IOException, ScriptException {
        Matcher m = JS_TABLE_PATTERN.matcher(tableConf.content);

        if (m.matches()) {
            if (!m.group(1).trim().equalsIgnoreCase(tableConf.name.trim())) {
                throw new RuntimeException("Created table must match with file name");
            }

            String sourceCode = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/" + clusterConf.name + "/" + m.group(2))));
            String inSchema = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/" + clusterConf.name + "/" + m.group(3))));
            String outSchema = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/" + clusterConf.name + "/" + m.group(4))));
            String invokeFunction = m.group(5);

            setupQueue(tableConf.name);

            JsTableSink sink = new JsTableSink(tableConf, new MobTableConfiguration(tableConf.name + "_in", inSchema, null), invokeFunction, sourceCode);
            JsTableSource source = new JsTableSource(tableConf, new MobTableConfiguration(tableConf.name + "_out", outSchema, null));

            tEnv.registerTableSink(sink.getName(), sink);
            AppendStreamTableUtils.createAndRegisterTableSourceDoMaterializeAsAppendStream(tEnv, source, source.getName());

        } else {
            throw new RuntimeException("Failed to create js table. They must conform to: " + JS_TABLE_PATTERN_REGEX + "\nSQL was: \n" + tableConf);
        }
    }

    private static Map<String, BlockingQueue<BlockingQueue<Map>>> queuesSink = new HashMap<>();
    private static Map<String, BlockingQueue<BlockingQueue<Map>>> queuesSource = new HashMap<>();

    // TODO remove, useful??
    private static AtomicInteger registrationTarget = new AtomicInteger(0);
    private static AtomicInteger counter = new AtomicInteger(0);

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
