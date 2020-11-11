package paris.benoit.mob.cluster.loopback.distributed;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterSender;
import paris.benoit.mob.server.ClusterSenderRegistry;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class KafkaClusterSenderRegistry implements ClusterSenderRegistry {

    private MobClusterConfiguration configuration;
//    private KafkaContainer kafka;
    Properties props = new Properties();

    {
//        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));
//        kafka.start();
//        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        // org.apache.kafka.common.serialization.ByteArraySerializer pour du zéro copy?
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("key.serializer","org.apache.kafka.connect.json.JsonSerializer");
//        props.put("value.serializer","org.apache.kafka.connect.json.JsonSerializer");
//        props.put("key.deserializer","org.apache.kafka.connect.json.JsonDeserializer");
//        props.put("value.deserializer","org.apache.kafka.connect.json.JsonDeserializer");
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

        // TODO put somewhere else:
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // TODO get value from ??
        // TODO get static local JVM UUID
        consumer.subscribe(Collections.singleton("mobcatalog.ack.send_client"));
        // TODO on peut subscribe avec un wildcard?
//        consumer.subscribe(Collections.singleton("mobcatalog.ack.send_client"));

        new Thread() {
            @Override
            public void run() {

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    records.records("mobcatalog.ack.send_client").forEach((it) -> {
                        configuration.clusterReceiver.receiveMessage(ToClientMessage.fromString(it.value()));
                    });
                }


            }
            // TODO et comment on stoppe gracefully?
        }.start();

    }

    @Override
    public CompletableFuture<Map<String, ClusterSender>> getClusterSenders(String random) {
        return CompletableFuture.supplyAsync(() -> {
            HashMap<String, ClusterSender> result = new HashMap<>();
            // TODO get value from client
            result.put("mobcatalog.ack.write_value", new ClusterSender() {

                final KafkaProducer<String, String> producer = new KafkaProducer(props);
                @Override
                public void sendMessage(ToServerMessage message) throws Exception {

                    ProducerRecord<String, String> msg = new ProducerRecord<>("mobcatalog.ack.write_value", message.toJsonString());
                    producer.send(msg);
                    System.out.println("sent : " + msg);
                }

                @Override
                public ToServerMessage receive() throws Exception {
                    return null;
                }
            });
            return result;
        });

    }
}
