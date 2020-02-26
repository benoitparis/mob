package paris.benoit.mob.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MobAppConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(MobAppConfiguration.class);

    public final String name;
    public final List<MobTableConfiguration> inSchemas;
    public final List<MobTableConfiguration> outSchemas;
    public final List<MobTableConfiguration> sql;
    public final List<MobTableConfiguration> tests;
    private final String basePath;

    public MobAppConfiguration(String name) {
        this.name = name;
        this.basePath = System.getProperty("user.dir") + "/apps/" + name + "/";
        logger.info("Configuration with basePath:" + basePath);

        this.inSchemas = buildConfigurationItem("in-schemas");
        this.outSchemas = buildConfigurationItem("out-schemas");
        this.sql = buildConfigurationItem("sql");
        this.tests = buildConfigurationItem("tests");
    }

    private List<MobTableConfiguration> buildConfigurationItem(final String folder) throws RuntimeException {

        Spliterator<Path> files;
        try {
            files = Files
                    .newDirectoryStream(Paths.get(basePath).resolve(folder))
                    .spliterator();
        } catch (IOException e) {
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
                                        name,
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
