package paris.benoit.mob.cluster;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    
    protected List<ConfigurationItem> inSchemas;
    protected List<ConfigurationItem> outSchemas;
    protected List<ConfigurationItem> states;
    protected List<ConfigurationItem> queries;

    private String basePath;

    public MobClusterConfiguration(String appName, UnderTowLauncher underTowLauncher, TimeCharacteristic processingtime, int streamParallelism, int maxBufferTimeMillis, int flinkWebUiPort) throws IOException {
        super();
        this.name = appName;
        this.processingtime = processingtime;
        this.streamParallelism = streamParallelism;
        this.maxBufferTimeMillis = maxBufferTimeMillis;
        this.flinkWebUiPort = flinkWebUiPort;
        
        this.underTowLauncher = underTowLauncher;
        
        this.basePath = System.getProperty("user.dir") + "/apps/" + appName + "/";
        logger.info("Configuration with basePath:" + basePath);
        
        this.inSchemas = buildConfigurationItem("in-schemas");
        this.outSchemas = buildConfigurationItem("out-schemas");
        this.states = buildConfigurationItem("states");
        this.queries = buildConfigurationItem("queries");
        
    }

    private List<ConfigurationItem> buildConfigurationItem(final String folder) throws IOException {
        
        return StreamSupport.stream(
                Files
                    .newDirectoryStream(Paths.get(basePath).resolve(folder))
                    .spliterator()
                , false
             )
            // FIXME alphabetical != ordering scheme 
            .sorted((a,b) -> a.getFileName().toString().compareTo(b.getFileName().toString()))
            .map(it -> {
                try {
                    return 
                        new ConfigurationItem(
                            new String(Files.readAllBytes(it)), 
                            it.getFileName().toString().split("\\.")[0]
                        );
                } catch (IOException e) {
                    // eww
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.toList());
    }
    
    public class ConfigurationItem {
        protected String content;
        protected String name;
        public ConfigurationItem(String content, String name) {
            super();
            this.content = content;
            this.name = name;
        }
    }

}
