package paris.benoit.mob.server;

import paris.benoit.mob.cluster.MobClusterConfiguration;

import java.util.List;

public interface ClusterFront {

    void start();

    default void waitReady() throws Exception {};

    String accessString();

    default void configure(MobClusterConfiguration configuration) {}

}
