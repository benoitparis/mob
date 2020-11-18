package paris.benoit.mob.server;

import paris.benoit.mob.cluster.MobClusterConfiguration;

import java.util.Map;

public interface ClusterSenderRegistry {

    void setConf(MobClusterConfiguration configuration);

    void waitRegistrationsReady();

    Map<String, ClusterSender> getClusterSenders();
}
