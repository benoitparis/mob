package paris.benoit.mob.cluster;

import paris.benoit.mob.server.ClusterFront;
import paris.benoit.mob.server.ClusterReceiver;
import paris.benoit.mob.server.ClusterSenderRegistry;

import java.util.List;
import java.util.stream.Collectors;

public class MobClusterConfiguration {

    final ClusterFront clusterFront;
    public final ClusterSenderRegistry clusterSenderRegistry;
    public final ClusterReceiver clusterReceiver;

    public final int streamParallelism;
    final int maxBufferTimeMillis;
    final Integer flinkWebUiPort;

    public final List<MobAppConfiguration> apps;

    public MobClusterConfiguration(
            List<String> names,
            ClusterFront clusterFront,
            ClusterReceiver clusterReceiver,
            ClusterSenderRegistry clusterSenderRegistry,
            int streamParallelism,
            int maxBufferTimeMillis,
            Integer flinkWebUiPort) {

        super();
        this.streamParallelism = streamParallelism;
        this.maxBufferTimeMillis = maxBufferTimeMillis;
        this.flinkWebUiPort = flinkWebUiPort;

        this.clusterFront = clusterFront;
        this.clusterSenderRegistry = clusterSenderRegistry;
        this.clusterReceiver = clusterReceiver;

        apps = names.stream().map(MobAppConfiguration::new).collect(Collectors.toList());

    }

}
