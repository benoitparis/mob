package paris.benoit.mob.cluster;

import org.apache.flink.streaming.api.TimeCharacteristic;
import paris.benoit.mob.server.ClusterFront;
import paris.benoit.mob.server.MessageRouter;

import java.util.List;
import java.util.stream.Collectors;

public class MobClusterConfiguration {

    public enum ENV_MODE {LOCAL, LOCAL_UI, REMOTE}

    protected ClusterFront clusterFront;
    protected MessageRouter router;

    protected TimeCharacteristic processingtime;
    protected int streamParallelism;
    protected int maxBufferTimeMillis;
    protected Integer flinkWebUiPort;
    protected ENV_MODE mode;

    public List<MobAppConfiguration> apps;

    public MobClusterConfiguration(
            List<String> names,
            ClusterFront clusterFront,
            MessageRouter router,
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
        this.router = router;

        apps = names.stream().map(MobAppConfiguration::new).collect(Collectors.toList());

    }

}
