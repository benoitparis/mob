package paris.benoit.mob.test;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobCluster;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.message.ToServerMessage;
import paris.benoit.mob.server.ClusterRunner;
import paris.benoit.mob.server.ClusterSender;

import java.util.ArrayList;
import java.util.List;

import static paris.benoit.mob.cluster.utils.Colors.green;
import static paris.benoit.mob.cluster.utils.Colors.red;

public class AppTestSuiteRunner implements ClusterRunner {
    private static final Logger logger = LoggerFactory.getLogger(AppTestSuiteRunner.class);

    private static AppTestFront front = new AppTestFront();

    @Override
    public void run(List<String> apps) throws Exception {

        for (String name : apps) {
            runRegistry(name);

            Boolean result = front.collectResult();
            if (result) {
                logger.info(green("All tests in the test suite passed"));
                // TODO PoisonPillException in soruce function to properly terminate? or poison message?
                // TODO rassembler les résultats et pas exit
                System.exit(0);
            } else {
                // FIXME ça s'affiche pas
                logger.info(red("A test in the test suite failed"));
                System.exit(-99);
            }
        }

    }


    private static void runRegistry(String name) throws Exception {

        MobClusterConfiguration configuration = new MobClusterConfiguration(
                new ArrayList<>() {{
                    add(name);
                }},
                front,
                new ClusterSender() {
                    @Override
                    public void sendMessage(ToServerMessage message) throws Exception {
                        throw new RuntimeException("TODO do something about me");
                    }
                    @Override
                    public ToServerMessage receive() throws Exception {
                        throw new RuntimeException("TODO do something about me");
                    }
                },
                TimeCharacteristic.IngestionTime,
                3,
                50,
                null,
                MobClusterConfiguration.ENV_MODE.LOCAL
        );

        logger.info("App to be tested: " + configuration.apps.get(0).name);
        front.setConfiguration(configuration.apps.get(0));

        MobCluster registry = new MobCluster(configuration);

        registry.start();

    }

}
