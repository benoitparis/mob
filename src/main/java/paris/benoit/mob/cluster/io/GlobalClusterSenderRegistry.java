package paris.benoit.mob.cluster.io;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterSender;

import java.util.Map;

public class GlobalClusterSenderRegistry {

    private static MobClusterConfiguration configuration;

    public static void setConf(MobClusterConfiguration configuration) {
        GlobalClusterSenderRegistry.configuration = configuration;
    }

    public static Map<String, ClusterSender> getClusterSenders() {
        return configuration.clusterSenderRegistry.getClusterSenders();
    }

}
