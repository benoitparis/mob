package paris.benoit.mob.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobClusterRegistry;
import paris.benoit.mob.cluster.MobClusterSender;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Map;

public class ClientSimulator {
    private static final Logger logger = LoggerFactory.getLogger(ClientSimulator.class);
    private static final Logger testSuiteLogger = LoggerFactory.getLogger("app-test-suite");

    private int progressCounter = 0;
    private String name;
    private String script;
    private Invocable inv;
    private Map<String, MobClusterSender> clusterSenders;


    public ClientSimulator(String name, String script) {
        this.name = name;
        this.script = script;
    }

    public void start() throws ScriptException {

        ScriptEngine graaljsEngine = new ScriptEngineManager().getEngineByName("graal.js");
        graaljsEngine.eval(script);
        inv = (Invocable) graaljsEngine;

        new Thread(() -> {
            try {
                clusterSenders = MobClusterRegistry.getClusterSender(name);
            } catch (InterruptedException e) {
                logger.debug("Problem getting a ClusterSender", e);
            }
        }).start();

    }

    public boolean progress() throws ScriptException, NoSuchMethodException {
        boolean doContinue = (boolean) inv.invokeFunction("progress", progressCounter);
        progressCounter++;
        logger.info("Client Simulator can continue progress: " + doContinue);


        return doContinue;


    }



}
