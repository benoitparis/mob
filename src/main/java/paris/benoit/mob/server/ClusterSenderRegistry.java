package paris.benoit.mob.server;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterSender;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ClusterSenderRegistry {
    void registerClusterSender(String fullName, ClusterSender sender, Integer loopbackIndex);

    void setConf(MobClusterConfiguration configuration);

    void waitRegistrationsReady() throws InterruptedException;

    CompletableFuture<Map<String, ClusterSender>> getClusterSenders(String random);
}
