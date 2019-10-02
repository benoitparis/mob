package paris.benoit.mob.cluster.table.js;
import paris.benoit.mob.cluster.MobTableConfiguration;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsTable {

    public static final String JS_TABLE_PATTERN_REGEX = "" +
            "CREATE TABLE ([^\\w]+)[\\w]+" +
            "LANGUAGE js[\\w]+" +
            "SOURCE '([^\\w]+)'[\\w]+" +
            "IN_SCHEMA '([^\\w]+)'[\\w]+" +
            "OUT_SCHEMA '([^\\w]+)'[\\w]+" +
            "INVOKE '([^\\w]+)'[\\w]+";
    public static final Pattern JS_TABLE_PATTERN = Pattern.compile(JS_TABLE_PATTERN_REGEX, Pattern.DOTALL);

    private String sourceCode;
    private String inSchema;
    private String outSchema;
    private String invokeFunction;


    ScriptEngine graaljsEngine = new ScriptEngineManager().getEngineByName("graal.js");
    Invocable inv = (Invocable) graaljsEngine;
    private JsTableSink sink;

    public JsTable(MobTableConfiguration conf) throws IOException, ScriptException {
        Matcher m = JS_TABLE_PATTERN.matcher(conf.content);

        if (m.matches()) {
            if (!m.group(1).trim().equalsIgnoreCase(conf.name.trim())) {
                throw new RuntimeException("Created table must match with file name");
            }
            // TODO gérer avec conf?
            sourceCode = new String(Files.readAllBytes(Paths.get(m.group(2))));
            inSchema = new String(Files.readAllBytes(Paths.get(m.group(3))));
            outSchema = new String(Files.readAllBytes(Paths.get(m.group(4))));
            invokeFunction = m.group(5);

            graaljsEngine.eval(sourceCode);
            registerEngine(conf.name, inv);

            sink = new JsTableSink(new MobTableConfiguration(conf.name + "_sink", inSchema));

        } else {
            throw new RuntimeException("Failed to create js table. They must conform to: " + JS_TABLE_PATTERN_REGEX + "\nSQL was: \n" + conf);
        }

    }




    static void registerEngine(String name, Invocable inv) {

    }

    static void registerSink(String name, JsSink jsTableSink) {

    }

    static void registerSource(String name, JsSource jsTableSink) {

    }

    static boolean isReady() {
        return false;
    }

}
