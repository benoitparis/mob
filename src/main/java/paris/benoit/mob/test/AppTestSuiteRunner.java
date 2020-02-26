package paris.benoit.mob.test;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobClusterRegistry;
import paris.benoit.mob.server.AppRunner;

import java.util.ArrayList;
import java.util.List;

public class AppTestSuiteRunner implements AppRunner {
    private static final Logger logger = LoggerFactory.getLogger(AppTestSuiteRunner.class);

    private static AppTestFront front = new AppTestFront();

    @Override
    public void run(List<String> apps) throws Exception {

        for (String name : apps) {
            runRegistry(name);

            Boolean result = front.collectResult();
            if (result) {
                logger.info("All tests in the test suite passed");
                // TODO PoisonPillException in soruce function to properly terminate? or poison message?
                // TODO rassembler les résultats et pas exit
                System.exit(0);
            } else {
                // FIXME ça s'affiche pas
                logger.info("A test in the test suite failed");
                System.exit(-99);
            }
        }

    }


    private static void runRegistry(String name) throws Exception {

        front = new AppTestFront();
        MobClusterConfiguration configuration = new MobClusterConfiguration(
                new ArrayList<String>() {{
                    add(name);
                }},
                front,
                new AppTestMessageRouter(),
                TimeCharacteristic.IngestionTime,
                3,
                50,
                null,
                MobClusterConfiguration.ENV_MODE.LOCAL
        );

        front.setConfiguration(configuration.apps.get(0));

        MobClusterRegistry registry = new MobClusterRegistry(configuration);

        registry.start();

    }

}
