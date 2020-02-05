package paris.benoit.mob.test;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobClusterRegistry;

public class AppTestSuiteRunner {
    private static final Logger logger = LoggerFactory.getLogger(AppTestSuiteRunner.class);

    private static AppTestFront front = new AppTestFront();
    private static MobClusterConfiguration configuration;

    public static void run(String name) throws Exception {
        runRegistry(name);

        Boolean result = front.collectResult();
        if (result) {
            logger.info("All tests in the test suite passed");
            // TODO PoisonPillException in soruce function to properly terminate? or poison message?
            System.exit(0);
        } else {
            logger.info("A test in the test suite failed");
            System.exit(-99);
        }
    }

    private static void runTests(String name) {
//        front.start(configuration);
    }

    private static void runRegistry(String name) throws Exception {
        configuration = new MobClusterConfiguration(
                name,
                front,
                new AppTestMessageRouter(),
                TimeCharacteristic.IngestionTime,
                3,
                50,
                null,
                MobClusterConfiguration.ENV_MODE.LOCAL
        );
        MobClusterRegistry registry = new MobClusterRegistry(configuration);

        registry.start();

    }

}
