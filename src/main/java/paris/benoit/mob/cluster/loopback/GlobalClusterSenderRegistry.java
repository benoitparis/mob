package paris.benoit.mob.cluster.loopback;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterSender;
import paris.benoit.mob.server.ClusterSenderRegistry;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GlobalClusterSenderRegistry {

    private static MobClusterConfiguration configuration;

    public static void setConf(MobClusterConfiguration configuration) {
        GlobalClusterSenderRegistry.configuration = configuration;
        configuration.clusterSenderRegistry.setConf(configuration);
    }

    public static void registerClusterSender(String fullName, ClusterSender sender, Integer loopbackIndex) {
        configuration.clusterSenderRegistry.registerClusterSender(fullName, sender, loopbackIndex);
    }

    public static void waitRegistrationsReady() throws InterruptedException {
        configuration.clusterSenderRegistry.waitRegistrationsReady();
    }

    public static CompletableFuture<Map<String, ClusterSender>> getClusterSenders(String random) {
        return configuration.clusterSenderRegistry.getClusterSenders(random);
    }

    public static class NameSenderPair {
        public final String fullName;
        // index c'est method-specific, hein
        final Integer loopbackIndex;
        public final ClusterSender sender;
        public NameSenderPair(String fullName, Integer loopbackIndex, ClusterSender sender) {
            this.fullName = fullName;
            this.loopbackIndex = loopbackIndex;
            this.sender = sender;
        }
        public Integer getLoopbackIndex() {
            return loopbackIndex;
        }
        @Override
        public String toString() {
            return "NameSenderPair{" + "fullName='" + fullName + '\'' + ", loopbackIndex=" + loopbackIndex + ", sender=" + sender + '}';
        }
    }
}
