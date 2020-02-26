package paris.benoit.mob.server;

import org.apache.flink.streaming.api.TimeCharacteristic;
import paris.benoit.mob.cluster.MobCluster;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.loopback.ActorMessageRouter;
import paris.benoit.mob.front.UndertowFront;

import java.util.List;

public class ServerRunner implements AppRunner {


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
                new UndertowFront(DEFAULT_FRONT_PORT),
                new ActorMessageRouter(),
                TimeCharacteristic.IngestionTime,
                DEFAULT_STREAM_PARALLELISM,
                DEFAULT_MAX_BUFFER_TIME_MILLIS,
                DEFAULT_FLINK_WEB_UI_PORT,
                MobClusterConfiguration.ENV_MODE.LOCAL_UI
        );
        MobCluster registry = new MobCluster(configuration);

        registry.start();

    }
}
