package paris.benoit.mob.test;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.loopback.ClusterRegistry;
import paris.benoit.mob.cluster.loopback.LocalQueueClusterSender;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterSender;

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
                clusterSenders = ClusterRegistry.getClusterSenders(name).get();
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

    public boolean progress() throws InterruptedException {
        boolean doContinue = progress.execute(progressCounter).asBoolean();
        progressCounter++;
        logger.info("Client Simulator can continue progress: " + doContinue);

        while(drainWsMessageQueue());
        lastClusterInteraction = System.currentTimeMillis();
        return doContinue;

    }

    private boolean drainWsMessageQueue() throws InterruptedException {

        String wsMessage = toServer.execute(progressCounter).asString();
        if (null == wsMessage) {
            return false;
        } else {

            // TODO DRY avec JettyWebSocketHandler
            ToServerMessage cMsg = new ToServerMessage(name, wsMessage);
            logger.info("Got message: " + cMsg);
            ClusterSender specificSender = clusterSenders.get(cMsg.table);
            if (null == specificSender) {
                logger.warn("A ClusterSender (table destination) was not found: " + cMsg.table);
            } else {
                try {
                    specificSender.sendMessage(cMsg);
                } catch (Exception e) {
                    logger.error("error in sending message", e);
                }
            }
            return true;
        }

    }

    public void offerMessage(ToClientMessage message) {
        if (name.equals(message.to)) {
            logger.info("Message offer matched: " + message.toString());
            lastClusterInteraction = System.currentTimeMillis();
            fromServer.execute(message.toJson());
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
