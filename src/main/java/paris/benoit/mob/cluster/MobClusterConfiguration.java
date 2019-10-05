package paris.benoit.mob.cluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.server.UnderTowLauncher;

public class MobClusterConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(MobClusterConfiguration.class);
    
    protected String name;
    
    protected UnderTowLauncher underTowLauncher;
    
    protected TimeCharacteristic processingtime;
    protected int streamParallelism;
    protected int maxBufferTimeMillis;
    protected int flinkWebUiPort;
    protected long latencyTrackingInterval;
    
    protected List<MobTableConfiguration> inSchemas;
    protected List<MobTableConfiguration> outSchemas;
    protected List<MobTableConfiguration> sql;

    private String basePath;


    public MobClusterConfiguration(String appName, UnderTowLauncher underTowLauncher, TimeCharacteristic processingtime, int streamParallelism, int maxBufferTimeMillis, int flinkWebUiPort, long latencyTrackingInterval) throws IOException {
        super();
        this.name = appName;
        this.processingtime = processingtime;
        this.streamParallelism = streamParallelism;
        this.maxBufferTimeMillis = maxBufferTimeMillis;
        this.flinkWebUiPort = flinkWebUiPort;
        this.latencyTrackingInterval = latencyTrackingInterval;
        
        this.underTowLauncher = underTowLauncher;
        
        this.basePath = System.getProperty("user.dir") + "/apps/" + appName + "/";
        logger.info("Configuration with basePath:" + basePath);
        
        this.inSchemas = buildConfigurationItem("in-schemas");
        this.outSchemas = buildConfigurationItem("out-schemas");
        this.sql = buildConfigurationItem("sql");
        
    }

    private List<MobTableConfiguration> buildConfigurationItem(final String folder) throws IOException {
        
        return StreamSupport.stream(
                Files
                    .newDirectoryStream(Paths.get(basePath).resolve(folder))
                    .spliterator()
                , false
             )
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
                    // eww
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

}
