package paris.benoit.mob.server;

import org.apache.flink.streaming.api.TimeCharacteristic;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobClusterRegistry;
import paris.benoit.mob.front.UndertowFront;

public class AppRunner {


    public final static int DEFAULT_STREAM_PARALLELISM = 4;
    // Apparamment à 1ms on est seulement 25% en dessous du max
    // https://flink.apache.org/2019/06/05/flink-network-stack.html
    public final static int DEFAULT_MAX_BUFFER_TIME_MILLIS = 5;
    public final static int DEFAULT_FRONT_PORT = 8090;
    public final static int DEFAULT_FLINK_WEB_UI_PORT = 8082;

    public static void run(String name) throws Exception {
        MobClusterConfiguration configuration = new MobClusterConfiguration(
                name,
                new UndertowFront(DEFAULT_FRONT_PORT),
                TimeCharacteristic.IngestionTime,
                DEFAULT_STREAM_PARALLELISM,
                DEFAULT_MAX_BUFFER_TIME_MILLIS,
                DEFAULT_FLINK_WEB_UI_PORT,
                MobClusterConfiguration.ENV_MODE.LOCAL_UI
        );
        MobClusterRegistry registry = new MobClusterRegistry(configuration);

        registry.start();

    }
}