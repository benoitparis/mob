package paris.benoit.mob.test;

import co.paralleluniverse.fibers.SuspendExecution;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.loopback.ClusterRegistry;
import paris.benoit.mob.cluster.loopback.ClusterSender;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.message.ToServerMessage;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

class ClientSimulator {
    private static final Logger logger = LoggerFactory.getLogger(ClientSimulator.class);

    private boolean isReady = false;
    private int progressCounter = 0;
    private final String name;
    private final String script;
    private Value progress;
    private Value toServer;
    private Value fromServer;
    private Value validate;
    private Map<String, ClusterSender> clusterSenders;
    private long lastClusterInteraction = System.currentTimeMillis();

    public String getName() {
        return name;
    }

    public ClientSimulator(String name, String script) {
        this.name = name;
        this.script = script;
    }

    public void start() throws IOException {

        logger.debug("Starting ClientSimulator " + name);

        Context context = Context
                .newBuilder("js")
                .build();
        Engine engine = context.getEngine();
        Source src = Source
                .newBuilder("js", script, name)
                .build();
        Value exports = context.eval(src);
        progress = exports.getMember("progress");
        toServer = exports.getMember("toServer");
        fromServer = exports.getMember("fromServer");
        validate = exports.getMember("validate");

        AppTestMessageRouter.registerClientSimulator(this);

        new Thread(() -> {
            try {
                clusterSenders = ClusterRegistry.getClusterSender(name).get();
                isReady = true;
            } catch (InterruptedException | ExecutionException e) {
                logger.debug("Problem getting a ClusterSender", e);
            }
        }).start();

    }

    public boolean isReady() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            logger.debug("Error while waiting", e);
        }
        return isReady;
    }

    public boolean progress() throws SuspendExecution, InterruptedException {
        boolean doContinue = progress.execute(progressCounter).asBoolean();
        progressCounter++;
        logger.info("Client Simulator can continue progress: " + doContinue);

        while(drainWsMessageQueue());
        lastClusterInteraction = System.currentTimeMillis();
        return doContinue;

    }

    private boolean drainWsMessageQueue() throws InterruptedException, SuspendExecution {

        String wsMessage = toServer.execute(progressCounter).asString();
        if (null == wsMessage) {
            return false;
        } else {

            // TODO DRY avec UndertowActor
            ToServerMessage cMsg = new ToServerMessage(wsMessage);
            logger.info("Got message: " + cMsg);
            switch (cMsg.intent) {
                case WRITE: {
                        ClusterSender specificSender = clusterSenders.get(cMsg.table);
                        if (null == specificSender) {
                            logger.warn("A ClusterSender (table destination) was not found: " + cMsg.table);
                        } else {
                            try {
                                specificSender.sendMessage(name, cMsg.payload.toString());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    break;
                case QUERY: logger.debug(cMsg.payload.toString());
                    break;
                case SUBSCRIBE: logger.debug(cMsg.payload.toString());
                    break;
            }
            return true;
        }

    }

    public void offerMessage(Integer loopbackIndex, String identity, ToClientMessage message) {
        if (name.equals(identity)) {
            logger.info("Message offer matched: " + loopbackIndex + "," + identity + ", message: " + message.toString());
            lastClusterInteraction = System.currentTimeMillis();
            fromServer.execute(message.toString());
        }
    }

    private static final long QUIET_MILLIS = 5_000;
    public boolean isQuiet() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            logger.debug("Error while waiting", e);
        }
        return System.currentTimeMillis() - lastClusterInteraction > QUIET_MILLIS;
    }

    public boolean validate() {
        return validate.execute(progressCounter).asBoolean();
    }
}
