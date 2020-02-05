package paris.benoit.mob.cluster;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.server.ClusterFront;
import paris.benoit.mob.server.MessageRouter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MobClusterConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(MobClusterConfiguration.class);

    public enum ENV_MODE {LOCAL, LOCAL_UI, REMOTE};

    public String name;

    protected ClusterFront clusterFront;
    protected MessageRouter router;

    protected TimeCharacteristic processingtime;
    protected int streamParallelism;
    protected int maxBufferTimeMillis;
    protected Integer flinkWebUiPort;
    protected ENV_MODE mode;

    protected List<MobTableConfiguration> inSchemas;
    protected List<MobTableConfiguration> outSchemas;
    protected List<MobTableConfiguration> sql;
    protected List<MobTableConfiguration> tests;

    private String basePath;

    public MobClusterConfiguration(
            String appName,
            ClusterFront clusterFront,
            MessageRouter router,
            TimeCharacteristic processingtime,
            int streamParallelism,
            int maxBufferTimeMillis,
            Integer flinkWebUiPort,
            ENV_MODE mode) throws IOException {

        super();
        this.name = appName;
        this.processingtime = processingtime;
        this.streamParallelism = streamParallelism;
        this.maxBufferTimeMillis = maxBufferTimeMillis;
        this.flinkWebUiPort = flinkWebUiPort;
        this.mode = mode;

        this.clusterFront = clusterFront;
        this.router = router;

        this.basePath = System.getProperty("user.dir") + "/apps/" + appName + "/";
        logger.info("Configuration with basePath:" + basePath);

        this.inSchemas = buildConfigurationItem("in-schemas");
        this.outSchemas = buildConfigurationItem("out-schemas");
        this.sql = buildConfigurationItem("sql");
        this.tests = buildConfigurationItem("tests");

    }

    private List<MobTableConfiguration> buildConfigurationItem(final String folder) throws IOException {

        Spliterator<Path> files;
        try {
            files = Files
                    .newDirectoryStream(Paths.get(basePath).resolve(folder))
                    .spliterator();
        } catch (NoSuchFileException e) {
            files = Spliterators.emptySpliterator();
        }

        return StreamSupport
            .stream(files, false)
            .filter(it -> !Files.isDirectory(it))
            .sorted(Comparator.comparing(a -> {
                try {
                    return Integer.valueOf(a.getFileName().toString().split("_")[0]);
                } catch (NumberFormatException e) {
                    return -1; // Arbitrary order
                }
            }
            ))
            .map(it -> {
                try {
                    String[] fileParts = it.getFileName().toString().split("\\.");
                    return
                        new MobTableConfiguration(
                            fileParts[0].replaceAll("^\\d*_", ""),
                            new String(Files.readAllBytes(it)),
                            getConfType(fileParts[1])
                        );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.toList());
    }

    private MobTableConfiguration.CONF_TYPE getConfType(String filePart) {
        try {
            return MobTableConfiguration.CONF_TYPE.valueOf(filePart.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public List<MobTableConfiguration> getTests() {
        return tests;
    }
}
