package paris.benoit.mob.cluster.loopback.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobAppConfiguration;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.loopback.GlobalClusterSenderRegistry;
import paris.benoit.mob.cluster.utils.TransferMap;
import paris.benoit.mob.server.ClusterSender;
import paris.benoit.mob.server.ClusterSenderRegistry;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Wrapper class, used for Sources to poll messages sent from the front.
 */
public class LocalQueueClusterSenderRegistry implements ClusterSenderRegistry {
    private static final Logger logger = LoggerFactory.getLogger(LocalQueueClusterSenderRegistry.class);

    private static MobClusterConfiguration configuration;
    private static CountDownLatch latch;
    private static final CopyOnWriteArrayList<GlobalClusterSenderRegistry.NameSenderPair> clusterSenderRaw = new CopyOnWriteArrayList<>();
    private static TransferMap<Integer, Map<String, ClusterSender>> transferMap = new TransferMap<>();

    @Override
    public void registerClusterSender(String fullName, ClusterSender sender, Integer loopbackIndex) {
        clusterSenderRaw.add(new GlobalClusterSenderRegistry.NameSenderPair(fullName, loopbackIndex, sender));
        latch.countDown();
    }

    @Override
    public void setConf(MobClusterConfiguration configuration) {
        LocalQueueClusterSenderRegistry.configuration = configuration;
        LocalQueueClusterSenderRegistry.latch = new CountDownLatch(configuration.streamParallelism * configuration.apps.stream().mapToInt(it -> it.inSchemas.size()).sum());

    }

    @Override
    public void waitRegistrationsReady() throws InterruptedException {
        latch.await();
        doClusterSendersMatching(configuration);
    }

    public void doClusterSendersMatching(MobClusterConfiguration configuration) {

        Map<String, List<GlobalClusterSenderRegistry.NameSenderPair>> byName = clusterSenderRaw.stream()
                .sorted(Comparator.comparing(GlobalClusterSenderRegistry.NameSenderPair::getLoopbackIndex))
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

    @Override
    public CompletableFuture<Map<String, ClusterSender>> getClusterSenders(String random) {
        return CompletableFuture.supplyAsync(() -> {
                    return transferMap.getAndWait(Math.abs(random.hashCode()) % configuration.streamParallelism);
                }
        );
    }

}