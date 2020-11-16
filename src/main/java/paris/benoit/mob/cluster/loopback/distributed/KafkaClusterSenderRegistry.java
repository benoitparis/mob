package paris.benoit.mob.cluster.loopback.distributed;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterSender;
import paris.benoit.mob.server.ClusterSenderRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaClusterSenderRegistry implements ClusterSenderRegistry {

    private MobClusterConfiguration configuration;
    Properties props = new Properties();

    {
        props.put("bootstrap.servers", "localhost:9092");
        // TODO mettre les mob.cluster-io.type distincts collectés
        props.put("group.id", "clients");
    }

    @Override
    public void setConf(MobClusterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void waitRegistrationsReady() {
        // TODO nothing to wait for, remove?
        // TODO refactor interface
        KafkaSchemaRegistry.getOutputSchemas().forEach((key, value) -> new KafkaClusterConsumer(props, key, configuration.clusterReceiver).start());

    }

    @Override
    public Map<String, ClusterSender> getClusterSenders(String random) {
        HashMap<String, ClusterSender> result = new HashMap<>();

        KafkaSchemaRegistry.getInputSchemas().forEach((key, value) -> result.put(key, new KafkaClusterSender(props, key)));

        return result;
    }
}
