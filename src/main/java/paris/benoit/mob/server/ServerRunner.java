package paris.benoit.mob.server;

import paris.benoit.mob.cluster.MobCluster;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.loopback.distributed.KafkaClusterSenderRegistry;
import paris.benoit.mob.front.JettyClusterReceiver;
import paris.benoit.mob.front.JettyFront;

import java.util.List;

public class ServerRunner implements ClusterRunner {

    private final static int DEFAULT_STREAM_PARALLELISM = 2;
    // Apparamment à 1ms on est seulement 25% en dessous du max
    // https://flink.apache.org/2019/06/05/flink-network-stack.html
    private final static int DEFAULT_MAX_BUFFER_TIME_MILLIS = 5;
    private final static int DEFAULT_FRONT_PORT = 8090;
    private final static int DEFAULT_FLINK_WEB_UI_PORT = 8082;

    @Override
    public void run(List<String> apps) throws Exception {
        MobClusterConfiguration configuration = new MobClusterConfiguration(
                apps,
                new JettyFront(DEFAULT_FRONT_PORT),
                new JettyClusterReceiver(),
                new KafkaClusterSenderRegistry(),
                DEFAULT_STREAM_PARALLELISM,
                DEFAULT_MAX_BUFFER_TIME_MILLIS,
                DEFAULT_FLINK_WEB_UI_PORT
        );

        // TODO change name, genre "deploy-app-job"
        MobCluster cluster = new MobCluster(configuration);
        cluster.start();

    }
}
