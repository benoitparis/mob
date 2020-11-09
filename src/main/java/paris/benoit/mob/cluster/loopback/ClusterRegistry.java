package paris.benoit.mob.cluster.loopback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobAppConfiguration;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.utils.TransferMap;
import paris.benoit.mob.server.ClusterSender;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// TODO DELETE
public class ClusterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ClusterRegistry.class);

    private static final CopyOnWriteArrayList<NameSenderPair> clusterSenderRaw = new CopyOnWriteArrayList<>();

    private static MobClusterConfiguration configuration;
    private static CountDownLatch latch;

    private static TransferMap<Integer, Map<String, ClusterSender>> transferMap = new TransferMap<>();

    public static void registerClusterSender(String fullName, ClusterSender sender, Integer loopbackIndex) {
        clusterSenderRaw.add(new NameSenderPair(fullName, loopbackIndex, sender));
        latch.countDown();
    }

    public static void setConf(MobClusterConfiguration configuration) {
        ClusterRegistry.configuration = configuration;
        ClusterRegistry.latch = new CountDownLatch(configuration.streamParallelism * configuration.apps.stream().mapToInt(it -> it.inSchemas.size()).sum());
    }

    public static void waitRegistrationsReady() throws InterruptedException {
        latch.await();
        doClusterSendersMatching(configuration);
    }

    private static void doClusterSendersMatching(MobClusterConfiguration configuration) {

        Map<String, List<NameSenderPair>> byName = clusterSenderRaw.stream()
                .sorted(Comparator.comparing(NameSenderPair::getLoopbackIndex))
                .collect(
                        Collectors.groupingBy(
                                it -> it.fullName,
                                Collectors.toList()
                        )
                );

        logger.info("Cluster senders names: " + byName.keySet());

        IntStream.range(0, configuration.streamParallelism).forEach(i -> {
            HashMap<String, ClusterSender> localMap = new HashMap<>();
            for (MobAppConfiguration app : configuration.apps) {
                for (MobTableConfiguration ci : app.inSchemas) {
                    localMap.put(ci.fullyQualifiedName(), byName.get(ci.fullyQualifiedName()).get(i).sender);
                }
                transferMap.put(i, localMap);
            }
        });

        logger.info("Registration Done");
    }


    public static CompletableFuture<Map<String, ClusterSender>> getClusterSenders(String random) {
        return CompletableFuture.supplyAsync(() -> {
                return transferMap.getAndWait(Math.abs(random.hashCode()) % configuration.streamParallelism);
            }
        );
    }

    static class NameSenderPair {
        final String fullName;
        final Integer loopbackIndex;
        final ClusterSender sender;
        NameSenderPair(String fullName, Integer loopbackIndex, ClusterSender sender) {
            this.fullName = fullName;
            this.loopbackIndex = loopbackIndex;
            this.sender = sender;
        }
        Integer getLoopbackIndex() {
            return loopbackIndex;
        }
        @Override
        public String toString() {
            return "NameSenderPair{" + "fullName='" + fullName + '\'' + ", loopbackIndex=" + loopbackIndex + ", sender=" + sender + '}';
        }
    }
}
