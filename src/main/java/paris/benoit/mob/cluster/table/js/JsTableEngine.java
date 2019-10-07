package paris.benoit.mob.cluster.table.js;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsTableEngine {
    private static final Logger logger = LoggerFactory.getLogger(JsTableEngine.class);

    public static final String JS_TABLE_PATTERN_REGEX = "" +
            "CREATE TABLE ([^\\s]+)[\\s]+" +
            "LANGUAGE js[\\s]+" +
            "SOURCE '([^\\s]+)'[\\s]+" +
            "IN_SCHEMA '([^\\s]+)'[\\s]+" +
            "OUT_SCHEMA '([^\\s]+)'[\\s]+" +
            "INVOKE '([^\\s]+)'[\\s]*";
    public static final Pattern JS_TABLE_PATTERN = Pattern.compile(JS_TABLE_PATTERN_REGEX, Pattern.DOTALL);

    public static void createAndRegister(StreamTableEnvironment tEnv, MobTableConfiguration conf) throws IOException, ScriptException {
        Matcher m = JS_TABLE_PATTERN.matcher(conf.content);

        if (m.matches()) {
            if (!m.group(1).trim().equalsIgnoreCase(conf.name.trim())) {
                throw new RuntimeException("Created table must match with file name");
            }
            // TODO gérer avec conf globale dans MobCLusterConfiguration
            String sourceCode = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/hw-pong/" + m.group(2))));
            String inSchema = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/hw-pong/" + m.group(3))));
            String outSchema = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/hw-pong/" + m.group(4))));
            String invokeFunction = m.group(5);

            ScriptEngine graaljsEngine = new ScriptEngineManager().getEngineByName("graal.js");
            graaljsEngine.eval(sourceCode);
            Invocable inv = (Invocable) graaljsEngine;
            registerEngine(conf.name, invokeFunction, inv);

            JsTableSink sink = new JsTableSink(conf, new MobTableConfiguration(conf.name + "_in", inSchema, null));
            JsTableSource source = new JsTableSource(conf, new MobTableConfiguration(conf.name + "_out", outSchema, null));

            tEnv.registerTableSink(sink.getName(), sink);

            tEnv.registerTableSource(source.getName() + "_raw", source);
            // TODO refactor ça avec le AppendStreamTableUTils
            Table rawTableSource = tEnv.sqlQuery(
                "SELECT\n" +
                "  " + StringUtils.join(source.getTableSchema().getFieldNames(), ",\n  ") + "\n" +
                "FROM " + source.getName() + "_raw" + "\n"
            );
            DataStream<Row> appendStream = tEnv
                    .toAppendStream(rawTableSource, source.getReturnType());
            logger.info("Registering as Table: " + source.getName());
            tEnv.registerTable(source.getName(), tEnv.fromDataStream(appendStream,
                StringUtils.join(source.getTableSchema().getFieldNames(), ", ") +
                ", proctime.proctime")
            );

        } else {
            throw new RuntimeException("Failed to create js table. They must conform to: " + JS_TABLE_PATTERN_REGEX + "\nSQL was: \n" + conf);
        }
    }

    static Map<String, BlockingQueue<BlockingQueue<Map>>> queues = new HashMap<>();
    static Map<String, BlockingQueue<Consumer<String>>> lambdas = new HashMap<>();

    // TODO remove, useful??
    static volatile int registrationTarget = 0;
    static volatile int counter = 0;

    static void registerEngine(String name, String invokeFunction, Invocable inv) {
        BlockingQueue<BlockingQueue<Map>> exchangeQueue = new ArrayBlockingQueue(1);
        BlockingQueue<Consumer<String>> exchangeLambda = new ArrayBlockingQueue(1);
        queues.put(name, exchangeQueue);
        lambdas.put(name, exchangeLambda);

        BlockingQueue<Map> queue = new ArrayBlockingQueue(100);
        Consumer<String> lambda = it -> {
            try {
                logger.debug("" + it);
                HashMap<String, String> map = new HashMap<>();
                map.put("dsadsa", "tretre");

                ObjectMapper mapper = new ObjectMapper();

//                queue.add((Map) inv.invokeFunction(invokeFunction, mapper.readTree("{\"haha\": 123}")));
                queue.add((Map) inv.invokeFunction(invokeFunction, it));
            } catch (ScriptException | NoSuchMethodException e) {
                throw new RuntimeException("Js invocation failed", e)
;            }
        };
        exchangeQueue.add(queue);
        exchangeLambda.add(lambda);

        registrationTarget = registrationTarget + 2;

    }

    static Consumer<String> registerSink(String name) throws InterruptedException {
        counter++;
        while (null == lambdas.get(name)){
            logger.warn("Waiting on registerSink, this shouldn't happen");
            Thread.sleep(100);
        }
        return lambdas.get(name).take();
    }

    static BlockingQueue<Map> registerSource(String name) throws InterruptedException {
        counter++;
        while (null == queues.get(name)){
            logger.warn("Waiting on registerSource, this shouldn't happen");
            Thread.sleep(100);
        }
        return queues.get(name).take();
    }

    public static boolean isReady() {
        logger.info("JsEngine Polled, registrationTarget = " + registrationTarget + ", counter = " + counter);
        return registrationTarget == counter;
    }

}
