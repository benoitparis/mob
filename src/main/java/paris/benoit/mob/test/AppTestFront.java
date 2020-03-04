package paris.benoit.mob.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobAppConfiguration;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.server.ClusterFront;
import paris.benoit.mob.server.ClusterReceiver;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class AppTestFront implements ClusterFront {
    private static final Logger logger = LoggerFactory.getLogger(AppTestFront.class);
    private List<Future<Boolean>> results;

    private MobAppConfiguration conf;

    @Override
    public void start() {

        this.results = conf.getTests()
            .stream()
            .map(this::getClientSimulator)
            .map(this::startAndGetResults)
            .collect(Collectors.toList())
        ;

    }

    private ClientSimulator getClientSimulator(MobTableConfiguration test) {
        logger.debug("Instantiating ClientSimulator");
        ClientSimulator client = new ClientSimulator("client", test.content);
        try {
            client.start();
        } catch (Exception e) {
            throw new RuntimeException("Exception in a ClientSimulator", e);
        }
        return client;
    }

    private Future<Boolean> startAndGetResults(ClientSimulator client) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        new Thread(() -> {
            try {
                client.start();
                while (!client.isReady()) ;
                while (client.progress()) ;
                while (!client.isQuiet()) ;

                result.complete(client.validate());

            } catch (Exception e) {
                throw new RuntimeException("Exception in a ClientSimulator", e);
            }
        }).start();
        return result;
    }

    @Override
    public void waitReady() {}

    @Override
    public String accessString() {
        return null;
    }

    @Override
    public ClusterReceiver getClusterReceiver() {
        return new AppTestMessageRouter();
    }

    public Boolean collectResult() {
        return results.stream()
            .map(it -> {
                try {
                    return it.get();
                } catch (Exception e) {
                    throw new RuntimeException("Exception in a ClientSimulator", e);
                }
            })
            .reduce(true, Boolean::logicalAnd)
        ;
    }

    public void setConfiguration(MobAppConfiguration configuration) {
        this.conf = configuration;
    }
}
