package paris.benoit.mob.server;

import org.apache.flink.streaming.api.TimeCharacteristic;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobClusterRegistry;

public class MobServer {
    
    public static void main(String[] args) throws Exception {
        if(getVersion() != 8) {
            System.out.println("Error: A Java 8 runtime must be used");
            System.out.println("Maven exec:exec goals can specify an executable path with: -Djava.executable=path/to/java");
            System.exit(-1);
        }

//        launchApp("ack");
//        launchApp("set-state-full-join");
//        launchApp("set-state-temporal-join");
//        launchApp("adder");
//        launchApp("tick");

        launchApp("pong");
    }

    public final static int STREAM_PARALLELISM = 4;
    // Apparamment à 1ms on est seulement 25% en dessous du max
    // https://flink.apache.org/2019/06/05/flink-network-stack.html
    public final static int MAX_BUFFER_TIME_MILLIS = 5;
    public final static int FRONT_PORT = 8090;
    public final static int FLINK_WEB_UI_PORT = 8082;
    public final static long LATENCY_TRACKING_INTERVAL = -1;
    
    public static void launchApp(String appName) throws Exception {

        MobClusterConfiguration configuration = new MobClusterConfiguration(
            appName,
            new UnderTowLauncher(FRONT_PORT),
            TimeCharacteristic.IngestionTime, 
            STREAM_PARALLELISM, 
            MAX_BUFFER_TIME_MILLIS,
            FLINK_WEB_UI_PORT,
            LATENCY_TRACKING_INTERVAL
        );
        MobClusterRegistry registry = new MobClusterRegistry(configuration);
        
        registry.start();
        
    }

    private static int getVersion() {
        String version = System.getProperty("java.version");
        System.out.println("Java version: " + version);
        if(version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if(dot != -1) { version = version.substring(0, dot); }
        } return Integer.parseInt(version);
    }
}
