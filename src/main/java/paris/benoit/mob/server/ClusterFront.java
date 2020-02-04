package paris.benoit.mob.server;

import paris.benoit.mob.cluster.MobClusterConfiguration;

public interface ClusterFront {

    void start(MobClusterConfiguration conf);

    void waitReady() throws InterruptedException;

    public String accessString();
}
