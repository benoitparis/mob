package paris.benoit.mob.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobCluster;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.server.ClusterRunner;
import paris.benoit.mob.server.ClusterSender;
import paris.benoit.mob.server.ClusterSenderRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static paris.benoit.mob.cluster.utils.Colors.green;
import static paris.benoit.mob.cluster.utils.Colors.red;

public class AppTestSuiteRunner implements ClusterRunner {
    private static final Logger logger = LoggerFactory.getLogger(AppTestSuiteRunner.class);

    private static AppTestFront front = new AppTestFront();
    private final static int DEFAULT_FLINK_WEB_UI_PORT = 8082;

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
                new AppTestMessageRouter(),
                new ClusterSenderRegistry() {
                    @Override
                    public void setConf(MobClusterConfiguration configuration) {}
                    @Override
                    public void waitRegistrationsReady() throws InterruptedException {}
                    @Override
                    public Map<String, ClusterSender> getClusterSenders(String random) {return null;}
                }, // TODO fix test
                3,
                50,
                DEFAULT_FLINK_WEB_UI_PORT
        );

        logger.info("App to be tested: " + configuration.apps.get(0).name);
        front.setConfiguration(configuration.apps.get(0));

        MobCluster registry = new MobCluster(configuration);

        registry.start();

    }

}
