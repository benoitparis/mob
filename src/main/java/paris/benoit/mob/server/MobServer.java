package paris.benoit.mob.server;

import org.apache.flink.streaming.api.TimeCharacteristic;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobClusterRegistry;

public class MobServer {
    
    public static void main(String[] args) throws Exception {
        setupCluster("hw-global-average");
//        setupCluster("hw-decaying");
//        setupCluster("hw-pong");
    }

    public final static int STREAM_PARALLELISM = 8;
    // Apparamment à 1ms on est seulement 25% en dessous du max
    // https://flink.apache.org/2019/06/05/flink-network-stack.html
    public final static int MAX_BUFFER_TIME_MILLIS = 1;
    public final static int FRONT_PORT = 8090;
    public final static int FLINK_WEB_UI_PORT = 8082;
    
    public static void setupCluster(String appName) throws Exception {

        MobClusterConfiguration configuration = new MobClusterConfiguration(
            appName,
            new UnderTowLauncher(FRONT_PORT),
            TimeCharacteristic.IngestionTime, 
            STREAM_PARALLELISM, 
            MAX_BUFFER_TIME_MILLIS,
            FLINK_WEB_UI_PORT
        );
        MobClusterRegistry registry = new MobClusterRegistry(configuration);
        registry.start();
        
    }

}
