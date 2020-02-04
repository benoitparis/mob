package paris.benoit.mob.test;

import org.apache.flink.streaming.api.TimeCharacteristic;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobClusterRegistry;

public class AppTestSuiteRunner {

    private static AppTestFront front = new AppTestFront();
    private static MobClusterConfiguration configuration;

    public static void run(String name) throws Exception {
        runRegistry(name);
        runTests(name);

        System.exit(0);
    }

    private static void runTests(String name) {
        front.start(configuration);
    }

    private static void runRegistry(String name) throws Exception {
        configuration = new MobClusterConfiguration(
                name,
                front,
                TimeCharacteristic.IngestionTime,
                2,
                50,
                null,
                MobClusterConfiguration.ENV_MODE.LOCAL
        );
        MobClusterRegistry registry = new MobClusterRegistry(configuration);

        registry.start();

    }

}
