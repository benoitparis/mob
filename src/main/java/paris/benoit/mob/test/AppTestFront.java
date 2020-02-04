package paris.benoit.mob.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterFront;

import javax.script.ScriptException;

public class AppTestFront implements ClusterFront {
    private static final Logger logger = LoggerFactory.getLogger(AppTestFront.class);
    private MobClusterConfiguration conf;

    @Override
    public void start(MobClusterConfiguration conf) {

        this.conf = conf;

        conf.getTests().stream().forEach(test -> {
            // get test conf
            String script = test.content;

            ClientSimulator client = new ClientSimulator("client", script);
            try {
                client.start();
                while(client.progress());
            } catch (ScriptException | NoSuchMethodException e) {
                logger.debug("Exception in a ClientSimulator", e);
            }

        });

    }

    @Override
    public void waitReady() throws InterruptedException {

    }

    @Override
    public String accessString() {
        return null;
    }


}
