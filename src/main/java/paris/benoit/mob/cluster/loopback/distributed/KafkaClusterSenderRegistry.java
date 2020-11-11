package paris.benoit.mob.cluster.loopback.distributed;

import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterSender;
import paris.benoit.mob.server.ClusterSenderRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class KafkaClusterSenderRegistry implements ClusterSenderRegistry {

    private MobClusterConfiguration configuration;
    Properties props = new Properties();

    {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "clients");
    }

    @Override
    public void registerClusterSender(String fullName, ClusterSender sender, Integer loopbackIndex) {
        // do nothing
    }

    @Override
    public void setConf(MobClusterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void waitRegistrationsReady() throws InterruptedException {
        // do nothing
        System.out.println(props);
        KafkaSchemaRegistry.getOutputSchemas().entrySet().stream()
            .forEach(it -> new KafkaClusterConsumer(props, it.getKey(), configuration.clusterReceiver).start());

    }

    @Override
    public CompletableFuture<Map<String, ClusterSender>> getClusterSenders(String random) {
        return CompletableFuture.supplyAsync(() -> {
            HashMap<String, ClusterSender> result = new HashMap<>();

            KafkaSchemaRegistry.getInputSchemas().entrySet().stream()
                .forEach(it -> result.put(it.getKey(), new KafkaClusterSender(props, it.getKey())));


            return result;
        });

    }
}
