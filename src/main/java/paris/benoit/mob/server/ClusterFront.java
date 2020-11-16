package paris.benoit.mob.server;

import paris.benoit.mob.cluster.MobClusterConfiguration;

public interface ClusterFront {

    void start();

    default void waitReady() {};

    String accessString();

    default void configure(MobClusterConfiguration configuration) {}

}
