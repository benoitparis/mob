package paris.benoit.mob.cluster;

import org.apache.flink.streaming.api.TimeCharacteristic;
import paris.benoit.mob.server.ClusterFront;
import paris.benoit.mob.server.ClusterReceiver;
import paris.benoit.mob.server.ClusterSender;

import java.util.List;
import java.util.stream.Collectors;

public class MobClusterConfiguration {

    public enum ENV_MODE {LOCAL, LOCAL_UI, REMOTE}

    final ClusterFront clusterFront;
    final ClusterSender clusterSender;
    final ClusterReceiver router;

    final TimeCharacteristic processingtime;
    public final int streamParallelism;
    final int maxBufferTimeMillis;
    final Integer flinkWebUiPort;

    final ENV_MODE mode;

    public final List<MobAppConfiguration> apps;

    public MobClusterConfiguration(
            List<String> names,
            ClusterFront clusterFront,
            ClusterSender clusterSender,
            TimeCharacteristic processingtime,
            int streamParallelism,
            int maxBufferTimeMillis,
            Integer flinkWebUiPort,
            ENV_MODE mode) {

        super();
        this.processingtime = processingtime;
        this.streamParallelism = streamParallelism;
        this.maxBufferTimeMillis = maxBufferTimeMillis;
        this.flinkWebUiPort = flinkWebUiPort;

        this.mode = mode;

        this.clusterFront = clusterFront;
        this.clusterSender = clusterSender;
        this.router = clusterFront.getClusterReceiver();

        apps = names.stream().map(MobAppConfiguration::new).collect(Collectors.toList());

    }

}
