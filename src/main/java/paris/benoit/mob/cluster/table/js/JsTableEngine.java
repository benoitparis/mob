package paris.benoit.mob.cluster.table.js;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsTableEngine {

    public static final String JS_TABLE_PATTERN_REGEX = "" +
            "CREATE TABLE ([^\\s]+)[\\s]+" +
            "LANGUAGE js[\\s]+" +
            "SOURCE '([^\\s]+)'[\\s]+" +
            "IN_SCHEMA '([^\\s]+)'[\\s]+" +
            "OUT_SCHEMA '([^\\s]+)'[\\s]+" +
            "INVOKE '([^\\s]+)'[\\s]*";
    public static final Pattern JS_TABLE_PATTERN = Pattern.compile(JS_TABLE_PATTERN_REGEX, Pattern.DOTALL);


    private String sourceCode;
    private String inSchema;
    private String outSchema;
    private String invokeFunction;

    ScriptEngine graaljsEngine = new ScriptEngineManager().getEngineByName("graal.js");
    Invocable inv = (Invocable) graaljsEngine;

    private JsTableSink sink;
    private JsTableSource source;

    public JsTableSink getSink() {
        return sink;
    }
    public JsTableSource getSource() {
        return source;
    }

    public JsTableEngine(MobTableConfiguration conf) throws IOException, ScriptException {
        Matcher m = JS_TABLE_PATTERN.matcher(conf.content);

        if (m.matches()) {
            if (!m.group(1).trim().equalsIgnoreCase(conf.name.trim())) {
                throw new RuntimeException("Created table must match with file name");
            }
            // TODO gérer avec conf globale dans MobCLusterConfiguration
            sourceCode = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/hw-pong/" + m.group(2))));
            inSchema = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/hw-pong/" + m.group(3))));
            outSchema = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/apps/hw-pong/" + m.group(4))));
            invokeFunction = m.group(5);

            graaljsEngine.eval(sourceCode);
            registerEngine(conf.name, invokeFunction, inv);

            sink = new JsTableSink(new MobTableConfiguration(inSchema, conf.name + "_in"));
            source = new JsTableSource(new MobTableConfiguration(outSchema, conf.name + "_out"));

        } else {
            throw new RuntimeException("Failed to create js table. They must conform to: " + JS_TABLE_PATTERN_REGEX + "\nSQL was: \n" + conf);
        }
    }

    static Map<String, BlockingQueue<Object>> queues = new HashMap<String, BlockingQueue<Object>>();
    static Map<String, Consumer<Object>> lambdas = new HashMap<String, Consumer<Object>>();

    static int registrationTarget = 0;
    static volatile int counter;

    static void registerEngine(String name, String invokeFunction, Invocable inv) {

        BlockingQueue<Object> queue = new ArrayBlockingQueue(100);
        Consumer<Object> lambda = it -> {
            try {
                queue.add(inv.invokeFunction("inJs", it));
            } catch (ScriptException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        };
        registrationTarget = registrationTarget + 2;

    }

    static Consumer<Object> registerSink(String name) {
        counter++;
        return lambdas.get(name);
    }

    static BlockingQueue<Object> registerSource(String name) {
        counter++;
        return queues.get(name);
    }

    public static boolean isReady() {
        return registrationTarget == counter;
    }

}
