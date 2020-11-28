package paris.benoit.mob.cluster.io;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterSender;
import paris.benoit.mob.server.ClusterSenderRegistry;

import java.util.Map;
import java.util.stream.Collectors;

public class KafkaClusterSenderRegistry implements ClusterSenderRegistry {

    private MobClusterConfiguration configuration;

    @Override
    public void setConf(MobClusterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void waitRegistrationsReady() {
        // TODO nothing to wait for, remove?
        // TODO refactor interface
        KafkaSchemaRegistry
                .getOutputSchemas()
                .forEach((key, value) -> new KafkaClusterConsumer(key, configuration.clusterReceiver).start());

    }

    @Override
    public Map<String, ClusterSender> getClusterSenders() {
        // TODO vu qu'on peut tout envoyer avec un producer, on peut le faire comme KafkaClusterSender sans trop de fuss (mais méthode de routage de schema?)
        return KafkaSchemaRegistry
                .getInputSchemas()
                .keySet()
                .stream()
                .collect(Collectors.toMap(key -> key, KafkaClusterSender::new));

    }
}
