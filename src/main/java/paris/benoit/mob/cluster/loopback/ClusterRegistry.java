package paris.benoit.mob.cluster.loopback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobAppConfiguration;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class ClusterRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ClusterRegistry.class);

    private static final CopyOnWriteArrayList<NameSenderPair> clusterSenderRaw = new CopyOnWriteArrayList<>();
    private static final List<Map<String, ClusterSender>> clusterSenders = new ArrayList<>();
    private static volatile boolean registrationDone = false;
    private static final int POLL_INTERVAL = 1000;

    public static void registerClusterSender(String fullName, ClusterSender sender, Integer loopbackIndex) {
        clusterSenderRaw.add(new NameSenderPair(fullName, loopbackIndex, sender));
    }

    public static void waitRegistrationsReady(int parallelism, MobClusterConfiguration configuration) throws InterruptedException {

        int inSchemaCount = configuration.apps.stream().mapToInt(it -> it.inSchemas.size()).sum();
        // On attend que tous les senders soient là
        // FIXME idéalement on fait que react à quand c'est prêt
        while ((clusterSenderRaw.size() != parallelism * inSchemaCount)) {
            logger.info("Waiting to receive all senders: " + clusterSenderRaw.size() + " != " + parallelism * inSchemaCount + " and JsTableEngines");
            logger.info("" + clusterSenderRaw);
            //logger.info("Plan is: \n" + sEnv.getExecutionPlan());
            Thread.sleep(POLL_INTERVAL);
        }
        doClusterSendersMatching(parallelism, configuration);
    }

    private static void doClusterSendersMatching(int parallelism, MobClusterConfiguration configuration) {

        Map<String, List<NameSenderPair>> byName = clusterSenderRaw.stream()
                .sorted(Comparator.comparing(NameSenderPair::getLoopbackIndex))
                .collect(
                        Collectors.groupingBy(
                                it -> it.fullName,
                                Collectors.toList()
                        )
                );

        logger.info("Cluster senders names: " + byName.keySet());

        for (int i = 0; i < parallelism; i++) {
            HashMap<String, ClusterSender> localMap = new HashMap<>();

            for(MobAppConfiguration app : configuration.apps) {
                for(MobTableConfiguration ci: app.inSchemas) {
                    localMap.put(ci.fullyQualifiedName(), byName.get(ci.fullyQualifiedName()).get(i).sender);
                }
                clusterSenders.add(localMap);
            }
        }

        registrationDone = true;
        logger.info("Registration Done");
    }

    public static Map<String, ClusterSender> getClusterSender(String random) throws InterruptedException {
        while (!registrationDone) {
            logger.info("Waiting on registration");
            Thread.sleep(POLL_INTERVAL);
        }

        return clusterSenders.get(Math.abs(random.hashCode()) % clusterSenders.size());
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
